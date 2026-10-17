*** Settings ***
Documentation     Narrative demonstration of the Operaton NiFi External Task integration.
...               Demonstrates process state in Operaton Cockpit, external task fetching
...               and locking in Apache NiFi, data transformation, BPMN token progression
...               with auto-refresh and sequence flow, and triumphant order fulfillment.
Library           screencast.Screencast    take_dir=${TAKE_DIR}    record=${RECORD}
Resource          resources/operaton_nifi.resource

*** Variables ***
${SHOTS_DIR}      ${TAKE_DIR}/screenshots

*** Tasks ***
Prepare The Take
    # Unrecorded: clean environment reset so every take starts identically
    Start Scratch Context    ${OPERATON_REST}/version
    Reset Demo State
    End Scratch Context

Start Observing Cockpit
    # Log in unrecorded, configure local storage for auto-refresh, and hand session to observer
    Start Scratch Context    ${OPERATON_URL}/app/cockpit/default/#/login
    Log In To Operaton    admin    admin
    ${state}=    Get Storage State
    End Scratch Context
    Start Observer    cockpit    ${OPERATON_URL}/app/cockpit/default/#/processes    storage_state=${state}
    Chapter    eyebrow=Operaton NiFi Orchestration · Part 1
    ...    title=BPMN Process & Order Arrival
    ...    subtitle=Operaton Cockpit process definition and active order ORD-1001
    Wait Until Visible    role=link[name="NiFi External Task Demo"]
    Human Click    role=link[name="NiFi External Task Demo"]
    Wait Until Visible    .diagram-holder
    Show Step    Order Fulfillment & Compliance BPMN definition: multi-stage external task orchestration    OPERATON
    Trace Process Route In Cockpit    StartEvent_1    Task_External
    Show Step    Order ORD-1001 (€149.99) received: token waiting at external task 'Validate & Enrich Order'    OPERATON
    # Open ORD-1001 instance view
    Navigate To Instance In Cockpit    ORD-1001
    Inspect External Task In Cockpit    Task_External
    Show Step    External task topic 'demo' is waiting for Apache NiFi worker to fetch and lock    OPERATON
    Inspect Variable In Cockpit    customer
    Inspect Variable In Cockpit    amount
    Inspect Variable In Cockpit    currency

Process Order In Apache NiFi
    [Setup]    Start Actor Turn    nifi    eyebrow=Operaton NiFi Orchestration · Part 2
    ...    title=Apache NiFi Pipeline
    ...    subtitle=Fetching, locking, validating, and completing external task ORD-1001
    ...    anonymous=${True}
    Go To    ${NIFI_URL}/
    Focus    actor    solo=${True}
    Wait Until Visible    \#canvas
    Show Step    Apache NiFi canvas: 3-column pipeline with Operaton External Task processors    APACHE NIFI
    Show Step    Locks are extended automatically by the External Task Service while the flow runs    APACHE NIFI
    # Fetch and lock task ORD-1001
    Show Step    Starting FetchAndLockExternalTasks: acquiring order ORD-1001 lease from Operaton    APACHE NIFI
    Start NiFi Processor    FetchAndLockExternalTasks
    Inspect Queue    FetchAndLockExternalTasks
    # Execute downstream validation and completion pipeline
    Show Step    Extracting JSON payload, validating business rules, and enriching metadata    APACHE NIFI
    Start NiFi Processor    Extract Order Attributes
    Start NiFi Processor    Validate Order Rules
    Start NiFi Processor    Enrich Order Metadata
    Inspect Queue    Enrich Order Metadata
    Show Step    CompleteExternalTask returns validated variables (validation.status = VALIDATED)    APACHE NIFI
    Start NiFi Processor    CompleteExternalTask
    Subtle Cursor Glide    1.0
    # Context switch: Focus observer where the action is (Operaton advances token live)
    Focus    observer    solo=${True}
    Show Step    Auto-refresh in Operaton: external task completes and token advances live!    OPERATON
    Refresh Cockpit View
    Show Step    Order amount (€149.99) < €500: token routes past gateway directly to Payment    OPERATON
    Inspect External Task In Cockpit    Task_Payment
    Inspect Variable In Cockpit    validation.status
    Inspect Variable In Cockpit    validation.worker
    # Context switch back: return focus to NiFi canvas to execute downstream payment & shipment
    Focus    actor    solo=${True}
    Pan NiFi Canvas    -850    0
    Show Step    Starting downstream fulfillment processors: Payment & Shipment tasks    APACHE NIFI
    Start NiFi Processor    Fetch Payment Tasks
    Start NiFi Processor    Fetch Shipment Tasks
    Start NiFi Processor    Complete Payment & Shipment
    Inspect Queue    Fetch Payment Tasks
    Subtle Cursor Glide    1.0
    # Context switch to observer for live fulfillment completion
    Focus    observer    solo=${True}
    Show Step    Downstream tasks completed: payment charged and shipment dispatched live!    OPERATON
    Refresh Cockpit View
    Subtle Cursor Glide    1.5
    Show Step    Order ORD-1001 reaches 'Order Fulfilled' — all stages complete!    OPERATON
    Subtle Cursor Glide    1.0
    [Teardown]    End Actor Turn

Review Completed Order Audit Trail
    Chapter    eyebrow=Operaton NiFi Orchestration · Part 3
    ...    title=End-to-End Fulfillment Audit
    ...    subtitle=Full execution history and green sequence flows in Operaton Cockpit
    Navigate To Instance History In Cockpit    ORD-1001
    Show Step    Order ORD-1001 fulfilled: complete execution audit trail in Operaton Cockpit    OPERATON
    Inspect External Task In Cockpit    EndEvent_1
    Trace Process Route In Cockpit    Task_External    EndEvent_1
    Inspect Variable In Cockpit    Order Fulfilled
    Inspect Variable In Cockpit    Validate & Enrich Order
    Show Step    Seamless distributed orchestration: Operaton BPM engine + Apache NiFi    OPERATON    duration=2.5
    Subtle Cursor Glide    3.0
    End Observer
