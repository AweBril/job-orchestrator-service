// package response contains methods for sending responses in common response formats.
package response

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
)

// ErrorMessage is the JSON response format for errors.
type ErrorMessage struct {
	Message string `json:"message"`
}

// missingResourceError creates a new missing resource error that describes a scenario when a request does not
// find the appropriate resource.
func missingResourceError(ty any, fieldname string, value any) ErrorMessage {
	return ErrorMessage{fmt.Sprintf("There is no %v with %v (%v)", reflect.TypeOf(ty).Name(), fieldname, value)}
}

// resourceExistsError creates a new resource exists error that describes when a unique resource constraint is violated.
func resourceExistsError(ty any, fieldname string, value any) ErrorMessage {
	return ErrorMessage{fmt.Sprintf("%v with %v (%v) already exists", reflect.TypeOf(ty).Name(), fieldname, value)}
}

// Missing sends a response for when a resource is missing.
func Missing(w http.ResponseWriter, ty any, fieldname string, value any) {
	JSON(w, http.StatusNotFound, missingResourceError(ty, fieldname, value))
}

// Exists sends a response for when a resource already exists, and therefore a new one cannot be created.
func Exists(w http.ResponseWriter, ty any, fieldname string, value any) {
	JSON(w, http.StatusConflict, resourceExistsError(ty, fieldname, value))
}

// InvalidRequest sends a response for when a request contains errors.
func InvalidRequest(w http.ResponseWriter, message string) {
	JSON(w, http.StatusBadRequest, ErrorMessage{message})
}

// InternalError sends a response for when there is internal error, and logs it.
func InternalError(w http.ResponseWriter) {
	JSON(w, http.StatusInternalServerError, ErrorMessage{"Internal Server Error"})
}

// New sends a response for when a new resource is successfully created.
func New(w http.ResponseWriter, data any) {
	JSON(w, http.StatusCreated, data)
}

// Empty sends an empty response.
func Empty(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNoContent)
}

// JSON sends a generic response as JSON.
func JSON(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}
