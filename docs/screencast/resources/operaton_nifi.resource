*** Settings ***
Documentation     Keywords for driving Operaton BPM and Apache NiFi in screencasts.

*** Variables ***
${OPERATON_URL}       http://127.0.0.1:8080/operaton
${OPERATON_REST}      http://127.0.0.1:8080/engine-rest
${NIFI_URL}           http://127.0.0.1:8081/nifi
${DEFAULT_USER}       admin
${DEFAULT_PASSWORD}   admin

*** Keywords ***
Log In To Operaton
    [Arguments]    ${username}=${DEFAULT_USER}    ${password}=${DEFAULT_PASSWORD}
    [Documentation]    Logs into Operaton Webapps Cockpit/Tasklist.
    Go To    ${OPERATON_URL}/app/cockpit/default/#/login
    Wait Until Visible    input[ng-model="username"]
    Human Type    input[ng-model="username"]    ${username}
    Human Type    input[ng-model="password"]    ${password}
    Human Click    role=button[name="Log in"]
    Wait For Navigation Away    ${OPERATON_URL}/app/cockpit/default/#/login

Navigate To Process In Cockpit
    [Arguments]    ${process_key}
    [Documentation]    Navigates to the process definition view in Operaton Cockpit.
    Go To    ${OPERATON_URL}/app/cockpit/default/#/process-definition/${process_key}
    Wait Until Visible    .diagram-holder

Inspect External Task In Cockpit
    [Arguments]    ${activity_id}
    [Documentation]    Clicks and highlights the external service task element.
    Human Move    .djs-element[data-element-id="${activity_id}"]
    Hold    2

Start NiFi Processor
    [Arguments]    ${processor_name}
    [Documentation]    Starts a processor on the NiFi canvas.
    Human Click    text="${processor_name}"
    Human Click    button[title="Start"]
    Wait Until Visible    .processor-running

Wait For FlowFiles In Queue
    [Arguments]    ${source_name}    ${target_name}
    [Documentation]    Polls the connection queue between two processors.
    [Tags]    screencast:wait
    Wait Until Visible    .connection-status:has-text("1 (")

Stop NiFi Processor
    [Arguments]    ${processor_name}
    [Documentation]    Stops a processor on the NiFi canvas.
    Human Click    text="${processor_name}"
    Human Click    button[title="Stop"]
