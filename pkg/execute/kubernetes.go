package execute

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"io"
	"time"

	"github.comcast.com/mesa/mafaas-job-orchestrator/pkg/orchestratorlog"
	"golang.org/x/exp/slog"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	batchv1client "k8s.io/client-go/kubernetes/typed/batch/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const IDLabel = "mafaas.id"

const (
	FailedReasonUnschedulable = "Pod cannot be created because it is unschedulable- most likely meaning that there are not enough resources available"
	FailedReasonJobFailed     = "Job itself in failed state"
)

// KubernetesExecutor is a component that manages Kubernetes "Jobs" as KubernetesTasks. It is meant only to handle
// the actual execution of a template as a task, and to provide an interface to view those tasks- any logic around
// scheduling, coordinating, and killing tasks should be handled elsewhere.
type KubernetesExecutor struct {
	templatesBucket         string
	namespace               string
	jobsClient              batchv1client.JobInterface
	podsClient              corev1client.PodInterface
	s3Svc                   *s3.S3
	nodeGroupMaxStartupTime map[string]time.Duration
}

// NewKubernetesExecutor creates a new KubernetesExecutor that looks for templates in S3 with the given session, and
// creates jobs from those templates in the given namespace.
func NewKubernetesExecutor(sess *session.Session, templatesBucket, jobsNamespace string) (*KubernetesExecutor, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed building kubernetes config: %w", err)
	}

	clients, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed getting kubernetes clients: %w", err)
	}

	return &KubernetesExecutor{
		templatesBucket:         templatesBucket,
		namespace:               jobsNamespace,
		jobsClient:              clients.BatchV1().Jobs(jobsNamespace),
		podsClient:              clients.CoreV1().Pods(jobsNamespace),
		s3Svc:                   s3.New(sess),
		nodeGroupMaxStartupTime: map[string]time.Duration{},
	}, nil
}

// SetNodeGroupMaxStartupTime stores the maximum startup time of any given nodegroup. This is done so that tasks do not
// report a failure when a node is starting, but not yet available.
func (e *KubernetesExecutor) SetNodeGroupMaxStartupTime(nodeGroup string, maxStartupTime time.Duration) {
	e.nodeGroupMaxStartupTime[nodeGroup] = maxStartupTime
}

// Execute executes a Go template located in S3 with "templateName" as the key, with the given parameters directly
// passed through. The id is assigned as a label to the newly created job.
func (e *KubernetesExecutor) Execute(ctx context.Context, id, templateName string, parameters any) (Task, error) {
	task, err := e.ReconnectTask(ctx, id)
	if err != nil {
		return nil, err
	}
	if task != nil {
		return nil, fmt.Errorf("task with given id %s already exists", id)
	}

	res, err := e.s3Svc.GetObject(&s3.GetObjectInput{
		Bucket: &e.templatesBucket,
		Key:    &templateName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed getting template '%s' from S3 bucket '%s': %w", templateName, e.templatesBucket, err)
	}
	defer res.Body.Close()

	templateBody, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read template '%s' from S3 bucket '%s': %w", templateName, e.templatesBucket, err)
	}

	tmpl, err := template.New(id).Parse(string(templateBody))
	if err != nil {
		return nil, fmt.Errorf("failed to parse job template (%v): %w", templateName, err)
	}

	buf := &bytes.Buffer{}
	err = tmpl.Execute(buf, parameters)
	if err != nil {
		return nil, fmt.Errorf("failed to execute job template (%v): %w", templateName, err)
	}

	obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(buf.Bytes(), nil, nil)
	if err != nil {
		return nil, err
	}
	job, ok := obj.(*batchv1.Job)
	if !ok {
		switch obj.(type) {
		default:
			return nil, fmt.Errorf("unexpected type: %T", obj)
		}
	}
	if job.Labels == nil {
		job.Labels = map[string]string{}
	}
	job.Labels[IDLabel] = id
	if job.Spec.Template.Labels == nil {
		job.Spec.Template.Labels = map[string]string{}
	}
	job.Spec.Template.Labels[IDLabel] = id

	job, err = e.jobsClient.Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed creating job: %w", err)
	}

	return &KubernetesTask{
		job:      job,
		executor: e,
	}, nil
}

// ReconnectTask checks if a task with the given ID already exists within the Kubernetes namespace associated with this
// executor. If it exists, that task will be returned. If it doesn't exist, nil will be returned with no error. An
// error is only returned if there was a failure with the connection to Kubernetes.
func (e *KubernetesExecutor) ReconnectTask(ctx context.Context, id string) (Task, error) {
	jobs, err := e.jobsClient.List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", IDLabel, id),
	})
	if err != nil {
		return nil, fmt.Errorf("failed listing jobs with id %s: %w", id, err)
	}
	if len(jobs.Items) < 1 {
		return nil, nil
	}
	if len(jobs.Items) > 1 {
		return nil, fmt.Errorf("!!!! id is not unique in Kubernetes, this should be impossible")
	}
	return &KubernetesTask{
		job:      &jobs.Items[0],
		executor: e,
	}, nil
}

// nodeStartupTime returns how long should be allowed for a given pod to wait for a node to be available.
func (e *KubernetesExecutor) nodeStartupTime(pod corev1.Pod) time.Duration {
	if startupTime, ok := e.nodeGroupMaxStartupTime[pod.Spec.NodeSelector["nodeType"]]; ok {
		return startupTime
	}
	return 0
}

// KubernetesTask wraps a Kubernetes Job as a logical unit of work within MAF. It allows the unique ID of the task to
// be retrieved, the simplified TaskState to be retrieved, and allows the job to be killed directly.
type KubernetesTask struct {
	job          *batchv1.Job
	executor     *KubernetesExecutor
	failedReason string
}

// GetID gets the ID of this task, as was given in the KubernetesExecutor.Execute function.
func (t *KubernetesTask) GetID() string {
	return t.job.Labels[IDLabel]
}

// State gets the simplified state of this Job, which generally follows the sequence:
//
//	"Starting" -> "Running" -> "Complete"
//	     |           V
//	     +--->    "Failed"
//
// Where "Failed" is any case *related to Kubernetes* that constitutes a failure. It does not recognize any logical
// errors within the Job's execution. The caller will have to manage those failures separately, using their knowledge
// of the executed template.
//
// After this function returns "Failed" as a state, the reason be retrieved from the FailedReason() function.
func (t *KubernetesTask) State(ctx context.Context) (TaskState, error) {
	if err := t.refresh(ctx); err != nil {
		return TaskStateUnknown, err
	}

	for _, cond := range t.job.Status.Conditions {
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
			t.failedReason = FailedReasonJobFailed
			return TaskStateFailed, nil
		}
		if cond.Type == batchv1.JobComplete && cond.Status == corev1.ConditionTrue {
			return TaskStateComplete, nil
		}
	}

	pods, err := t.executor.podsClient.List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("controller-uid=%s", t.job.UID),
	})
	if err != nil {
		return TaskStateUnknown, fmt.Errorf("failed to get state of job's pods: %w", err)
	}
	if len(pods.Items) < 1 {
		return TaskStateStarting, nil
	}

	pod := pods.Items[0]
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.ContainersReady &&
			cond.Status == corev1.ConditionTrue {
			return TaskStateRunning, nil
		}

		if cond.Type == corev1.PodScheduled &&
			cond.Status == corev1.ConditionFalse &&
			cond.Reason == corev1.PodReasonUnschedulable {
			if time.Since(cond.LastTransitionTime.Time) > t.executor.nodeStartupTime(pod) {
				t.failedReason = FailedReasonUnschedulable
				return TaskStateFailed, nil
			}
			break
		}
	}

	return TaskStateStarting, nil
}

// FailedReason gets the reason that the previous call of the State() function failed. Will be one of the "FailedReasonX"
// constants defined at the top of this file. Calls made before State() returns a TaskStateFailed will return the empty
// string.
func (t *KubernetesTask) FailedReason() string {
	return t.failedReason
}

// Kill stops the underlying job and deletes it from Kubernetes.
func (t *KubernetesTask) Kill(ctx context.Context) error {
	policy := metav1.DeletePropagationForeground
	err := t.executor.jobsClient.Delete(ctx, t.job.Name, metav1.DeleteOptions{
		PropagationPolicy: &policy,
	})
	if err != nil {
		return fmt.Errorf("failed deleting job from kubernetes: %w", err)
	}
	return nil
}

// refresh refreshes the content of the internal job struct so that the information it contains is as recent as possible.
func (t *KubernetesTask) refresh(ctx context.Context) error {
	job, err := t.executor.jobsClient.Get(ctx, t.job.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to refresh from remote kubernetes job: %w", err)
	}
	t.job = job
	return nil
}

// ECSLogAttributes returns the log attributes of this type.
func (t *KubernetesTask) ECSLogAttributes() []slog.Attr {
	return orchestratorlog.MAFJob{TaskID: t.GetID()}.ECSLogAttributes()
}
