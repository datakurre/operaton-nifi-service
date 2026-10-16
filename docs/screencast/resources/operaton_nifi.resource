*** Settings ***
Documentation     Keywords for driving Operaton BPM and Apache NiFi in screencasts.
Library           OperatonNiFiLibrary.py

*** Variables ***
${OPERATON_URL}       http://127.0.0.1:8080/operaton
${OPERATON_REST}      http://127.0.0.1:8080/engine-rest
${NIFI_URL}           http://127.0.0.1:8081/nifi
${DEFAULT_USER}       admin
${DEFAULT_PASSWORD}   admin

*** Keywords ***
Reset Demo State
    [Documentation]    Resets Operaton demo instances and NiFi canvas.
    Reset Demo Environment

Log In To Operaton
    [Arguments]    ${username}=${DEFAULT_USER}    ${password}=${DEFAULT_PASSWORD}
    [Documentation]    Logs into Operaton Webapps Cockpit/Tasklist and configures localStorage.
    Go To    ${OPERATON_URL}/app/cockpit/default/#/login
    Wait Until Visible    input[ng-model="username"]
    Human Type    input[ng-model="username"]    ${username}
    Human Type    input[ng-model="password"]    ${password}
    Human Click    role=button[name="Log in"]
    Wait For Navigation Away    ${OPERATON_URL}/app/cockpit/default/#/login
    Configure Operaton Local Storage

Navigate To Process In Cockpit
    [Arguments]    ${process_key}
    [Documentation]    Navigates to the process definition view in Operaton Cockpit.
    Go To    ${OPERATON_URL}/app/cockpit/default/#/process-definition/${process_key}
    Wait Until Visible    .diagram-holder
    Activate Cockpit Flow And Auto Refresh

Navigate To History In Cockpit
    [Arguments]    ${process_key}
    [Documentation]    Navigates to the completed process history view in Operaton Cockpit.
    Go To    ${OPERATON_URL}/app/cockpit/default/#/process-definition/${process_key}/history
    Wait Until Visible    .diagram-holder
    Activate Cockpit Flow And Auto Refresh

Activate Cockpit Flow And Auto Refresh
    [Documentation]    Clicks the flow and auto-refresh buttons in Operaton Cockpit if visible.
    Configure Operaton Local Storage
    ${has_seq}=    Count Matches    button.toggle-sequence-flow-button
    IF    ${has_seq} > 0
        Human Move    button.toggle-sequence-flow-button
        Human Click    button.toggle-sequence-flow-button
    END
    ${has_auto}=    Count Matches    button.toggle-auto-refresh-button
    IF    ${has_auto} > 0
        Human Move    button.toggle-auto-refresh-button
        Human Click    button.toggle-auto-refresh-button
    END

Navigate To Instance In Cockpit
    [Arguments]    ${business_key}
    [Documentation]    Navigates directly to the runtime instance view for a given order business key.
    ${inst_id}=    Get Operaton Instance Id By Key    ${business_key}
    Go To    ${OPERATON_URL}/app/cockpit/default/#/process-instance/${inst_id}
    Wait Until Visible    .diagram-holder
    Activate Cockpit Flow And Auto Refresh

Navigate To Instance History In Cockpit
    [Arguments]    ${business_key}
    [Documentation]    Navigates directly to the historical instance view for a completed order.
    ${inst_id}=    Get Operaton History Instance Id By Key    ${business_key}
    Go To    ${OPERATON_URL}/app/cockpit/default/#/history/process-instance/${inst_id}
    Wait Until Visible    .djs-container
    Activate Cockpit Flow And Auto Refresh

Inspect External Task In Cockpit
    [Arguments]    ${activity_id}
    [Documentation]    Clicks and highlights the external service task element.
    Highlight Bpmn Activity    ${activity_id}

Trace Process Route In Cockpit
    [Arguments]    ${from_id}    ${to_id}
    [Documentation]    Traces the sequence flow path between two BPMN elements.
    Trace Bpmn Flow    ${from_id}    ${to_id}

Inspect Variable In Cockpit
    [Arguments]    ${var_name}
    [Documentation]    Hovers over a variable row in the Cockpit variables table.
    Inspect Variable Row    ${var_name}

Refresh Cockpit View
    [Documentation]    Refreshes the Cockpit view to show live token progression.
    Trigger Cockpit Refresh

Start NiFi Processor
    [Arguments]    ${processor_name}
    [Documentation]    Starts a processor on the NiFi canvas via operation-control.
    Start NiFi Canvas Processor    ${processor_name}

Stop NiFi Processor
    [Arguments]    ${processor_name}
    [Documentation]    Stops a processor on the NiFi canvas via operation-control.
    Stop NiFi Canvas Processor    ${processor_name}

Inspect Queue
    [Arguments]    ${processor_name}
    [Documentation]    Hovers over the downstream queue of a processor.
    Inspect Nifi Queue    ${processor_name}

Highlight NiFi Processor
    [Arguments]    ${processor_name}
    [Documentation]    Selects and highlights a processor on the NiFi canvas.
    Highlight Canvas Processor    ${processor_name}

Show Step
    [Arguments]    ${text}    ${tag}=OPERATON    ${duration}=4.0
    [Documentation]    Displays a floating narrative banner across contexts and emits WebVTT caption.
    Show Narrative Banner    ${text}    ${tag}
    Caption    ${text}    ${duration}
