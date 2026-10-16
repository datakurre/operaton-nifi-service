*** Settings ***
Documentation     Multi-actor screencast demonstrating the Operaton NiFi External Task worker.
...               Shows process token in Cockpit, NiFi locking, FlowFile transformation,
...               and task completion with updated process variables.
Library           screencast.Screencast    take_dir=${TAKE_DIR}    record=${RECORD}
Resource          resources/operaton_nifi.resource

*** Variables ***
${SHOTS_DIR}      ${TAKE_DIR}/screenshots

*** Tasks ***
Prepare The Take
    # Unrecorded: reset state, start process instances via REST API
    Start Scratch Context    http://127.0.0.1:8080/engine-rest
    End Scratch Context

Start Observing Cockpit
    # Log in unrecorded, then pass storage state to the observer
    Start Scratch Context    http://127.0.0.1:8080/operaton/app/cockpit/default/#/login
    Log In To Operaton    admin    admin
    ${state}=    Get Storage State
    End Scratch Context
    Start Observer    cockpit    http://127.0.0.1:8080/operaton/app/cockpit/default/    storage_state=${state}

Show Process In Cockpit
    [Setup]    Start Actor Turn    cockpit    eyebrow=Operaton NiFi · Part 1
    ...    title=Operaton Cockpit    subtitle=Process instance waiting on external task "demo"
    Go To    http://127.0.0.1:8080/operaton/app/cockpit/default/#/process-definition
    Wait Until Visible    .table
    Hold    2
    [Teardown]    End Actor Turn

Execute Task In Apache NiFi
    [Setup]    Start Actor Turn    nifi    eyebrow=Operaton NiFi · Part 2
    ...    title=Apache NiFi    subtitle=Fetching, locking, and completing the external task
    Go To    http://127.0.0.1:8081/nifi/
    Wait Until Visible    .graph-control
    Human Move    text="FetchAndLockExternalTasks"
    Hold    1
    Start NiFi Processor    FetchAndLockExternalTasks
    Hold    2
    Start NiFi Processor    CompleteExternalTask
    Hold    2
    [Teardown]    End Actor Turn

Verify Process Completion
    [Setup]    Start Actor Turn    cockpit    eyebrow=Operaton NiFi · Part 3
    ...    title=Process Advancing    subtitle=External task completed and variables updated
    Observe    http://127.0.0.1:8080/operaton/app/cockpit/default/#/history
    Wait Until Visible    .history-view
    Hold    3
    [Teardown]    End Actor Turn

Wrap Up
    End Observer
