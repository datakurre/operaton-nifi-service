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
    # Unrecorded: ensure demo process is deployed and instance is waiting
    Start Scratch Context    http://127.0.0.1:8080/engine-rest
    End Scratch Context

Start Observing Cockpit
    # Log in unrecorded, then pass storage state to the observer
    Start Scratch Context    http://127.0.0.1:8080/operaton/app/cockpit/default/#/login
    Log In To Operaton    admin    admin
    ${state}=    Get Storage State
    End Scratch Context
    Start Observer    cockpit    http://127.0.0.1:8080/operaton/app/cockpit/default/#/processes    storage_state=${state}
    Chapter    eyebrow=Operaton NiFi · Part 1
    ...    title=Operaton Cockpit    subtitle=Process instance waiting on external task "demo"
    Wait Until Visible    role=link[name="NiFi External Task Demo"]
    Human Click    role=link[name="NiFi External Task Demo"]
    Wait Until Visible    .diagram-holder
    Hold    3

Execute Task In Apache NiFi
    [Setup]    Start Actor Turn    nifi    eyebrow=Operaton NiFi · Part 2
    ...    title=Apache NiFi    subtitle=Fetching, locking, and completing the external task
    ...    anonymous=${True}
    Go To    http://127.0.0.1:8081/nifi/
    Wait Until Visible    \#canvas
    Human Move    text="FetchAndLockExternalTasks"
    Hold    2
    Human Move    text="CompleteExternalTask"
    Hold    2
    [Teardown]    End Actor Turn

Verify Process Completion
    Chapter    eyebrow=Operaton NiFi · Part 3
    ...    title=Process Advancing    subtitle=External task completed and variables updated
    Observe    http://127.0.0.1:8080/operaton/app/cockpit/default/#/processes
    Wait Until Visible    role=link[name="NiFi External Task Demo"]
    Hold    3

Wrap Up
    Hold    2
    End Observer
