# Job Orchestrator
Recieves requests to run MAF jobs, and runs those jobs according to their parameters. Currently supports both Live jobs and most of File jobs.

## Pushing Docker Image
```
make sync
```

### ssh-agent
In order to safely pass in credentials, docker buildkit allows us to pass in keys safely through `ssh-agent`.
If you are unfamiliar with this, this is the process to set that up:
```
eval `ssh-agent`
ssh-add [whichever private key you use for github]
```

You can then run any commands that require ssh without entering a passphrase, allowing the docker container to
use the keys as well.

When/if you don't want the ssh-agent to continue running:
```
kill $SSH_AGENT_PID
```

#### sudo

If your `docker` command requires `sudo` permissions, you will need to either:
1. Add the line `Defaults    env_keep+=SSH_AUTH_SOCK` to your `/etc/sudoers` file, and never have to worry about this again, or
2. Run the command like so: `sudo -E -s make sync`. This preserves environmental variables for the executed command.

For more information on this, check out [this post](https://serverfault.com/questions/107187/ssh-agent-forwarding-and-sudo-to-another-user).
