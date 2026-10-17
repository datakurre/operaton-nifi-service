*** Settings ***
Documentation     Narrative demonstration of the Finnish Travel & Outdoor Adventure Planner.
...               Demonstrates 1-click presets in Operaton Tasklist, deterministic Open-Meteo
...               geocoding and live Finnish weather, Wikipedia cultural bio, human curation
...               with wood-fired sauna and regional dining choices, and dispatch to Mailpit.
Library           screencast.Screencast    take_dir=${TAKE_DIR}    record=${RECORD}
Resource          resources/operaton_nifi.resource

*** Tasks ***
Prepare The Take
    # Clean environment reset so every take starts identically
    Start Scratch Context    ${OPERATON_REST}/version
    Prepare Travel Planner State
    End Scratch Context

Start Observing Tasklist
    Start Observer    tasklist    ${OPERATON_URL}/app/tasklist/default/#/login
    Chapter    eyebrow=Operaton BPMN & Apache NiFi 2.4.0 · Part 1
    ...    title=Finnish Travel & Adventure Planner
    ...    subtitle=1-Click Start Form Presets in Operaton Tasklist
    Log In To Operaton Tasklist    admin    admin
    Show Step    Operaton Tasklist: 1-click start form presets for iconic Finnish destinations    OPERATON
    Start Process In Tasklist    koli
    Show Step    Launched trip instance for Koli National Park — token waiting for NiFi workers    OPERATON
    Subtle Cursor Glide    1.0

Observe NiFi Pipeline
    [Setup]    Start Actor Turn    nifi    eyebrow=Operaton BPMN & Apache NiFi 2.4.0 · Part 2
    ...    title=Real-Time Geocoding & Live FMI Weather
    ...    subtitle=Open-Meteo & Wikipedia open APIs in Apache NiFi 2.4.0
    ...    anonymous=${True}
    ${pg_id}=    Get Travel Planner Group Id
    Go To    ${NIFI_URL}/?processGroupId=${pg_id}
    Focus    actor    solo=${True}
    Wait Until Visible    \#canvas
    Show Step    Apache NiFi: 4-column flow with Operaton External Task processors    APACHE NIFI
    Highlight NiFi Processor    Geocode Open-Meteo
    Show Step    Column 1 (fin-trip-lookup): resolves coordinates with Open-Meteo Geocoding    APACHE NIFI
    Subtle Cursor Glide    1.0
    Pan NiFi Canvas    -500    0
    Highlight NiFi Processor    Fetch Open-Meteo FMI Weather
    Highlight NiFi Processor    Fetch Wikipedia Profile
    Show Step    Column 2 (fin-weather-check): live Finnish weather & Wikipedia cultural dossier    APACHE NIFI
    Subtle Cursor Glide    1.5
    [Teardown]    End Actor Turn

Curate Trip In Tasklist
    Focus    observer    solo=${True}
    Show Step    Token arrives at User Task: traveler curates stay style, sauna and dining plan    OPERATON
    Open And Claim Curation Task
    Show Step    Pre-populated with live weather, daylight window, and Wikipedia photo thumbnail    OPERATON
    Fill Curation Choices And Submit
    Show Step    Traveler approves Koli adventure plan: campfire cooking & lakeside mökki with sauna    OPERATON
    Subtle Cursor Glide    1.5

Observe Itinerary Compilation And Dispatch
    [Setup]    Start Actor Turn    nifi    eyebrow=Operaton BPMN & Apache NiFi 2.4.0 · Part 3
    ...    title=Gastronomy Mapping & Multichannel Dispatch
    ...    subtitle=Compiling Karjalanpiirakat & muikku dinner, PutEmail & PutFile archive
    ...    anonymous=${True}
    ${pg_id}=    Get Travel Planner Group Id
    Go To    ${NIFI_URL}/?processGroupId=${pg_id}
    Focus    actor    solo=${True}
    Wait Until Visible    \#canvas
    Pan NiFi Canvas    -1100    0
    Highlight NiFi Processor    Map Regional Food & Gear
    Highlight NiFi Processor    Render Adventure Markdown
    Show Step    Column 3 (fin-itinerary-compile): maps Karelian pies, muikku dinner & gear list    APACHE NIFI
    Subtle Cursor Glide    1.0
    Pan NiFi Canvas    -500    0
    Highlight NiFi Processor    Deliver Itinerary to Mailpit
    Highlight NiFi Processor    Archive Itinerary Markdown
    Show Step    Column 4 (fin-itinerary-dispatch): PutEmail dispatches to Mailpit; PutFile archives guide    APACHE NIFI
    Subtle Cursor Glide    1.5
    [Teardown]    End Actor Turn

Inspect Delivered Email In Mailpit
    [Setup]    Start Actor Turn    mailpit    eyebrow=Operaton BPMN & Apache NiFi 2.4.0 · Part 4
    ...    title=Delivered Itinerary in Mailpit
    ...    subtitle=Clean Markdown briefing delivered to local inbox
    ...    anonymous=${True}
    Focus    actor    solo=${True}
    Open Mailpit And View Itinerary
    Show Step    Itinerary delivered to Mailpit: daily trail route, campfire lunch, fika & sauna plunge!    MAILPIT
    Subtle Cursor Glide    2.5
    [Teardown]    End Actor Turn

Conclude Screencast
    Focus    observer    solo=${True}
    Chapter    eyebrow=Operaton BPMN + Apache NiFi 2.4.0
    ...    title=Finnish Travel Planner Orchestrated Successfully
    ...    subtitle=Zero heavy AI, zero SQL, deterministic open APIs, and human-in-the-loop task curation
    Show Step    Operaton BPMN + Apache NiFi 2.4.0: Reliable, deterministic, human-in-the-loop orchestration    OPERATON    duration=3.0
    Subtle Cursor Glide    2.0
    End Observer
