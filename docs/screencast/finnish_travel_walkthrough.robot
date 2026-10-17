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
    ...    subtitle=Deterministic Open-Meteo & Wikimedia open APIs in Apache NiFi 2.4.0
    ...    anonymous=${True}
    ${pg_id}=    Get Travel Planner Group Id
    Go To    ${NIFI_URL}/#/process-groups/${pg_id}
    Focus    actor    solo=${True}
    Wait Until Visible    \#canvas
    Show Step    Apache NiFi: 4-column flow with Operaton External Task processors    APACHE NIFI
    Highlight NiFi Processor    Fetch Destination Task
    Show Step    Column 1 (fin-trip-lookup): acquires task lease and extracts destination preset    APACHE NIFI
    Highlight NiFi Processor    Geocode Open-Meteo
    Show Step    Invokes Open-Meteo Geocoding API to resolve coordinates for Lieksa / Koli    APACHE NIFI
    Highlight NiFi Processor    Validate Destination Location
    Show Step    RouteOnAttribute verifies Finland coordinates; routes unknown cities to BPMN error    APACHE NIFI
    Highlight NiFi Processor    Complete Destination Lookup
    Show Step    CompleteExternalTask returns resolved lat/lon to Operaton token via REST    APACHE NIFI
    Highlight NiFi Processor    Fetch Weather Task
    Show Step    Column 2 (fin-weather-check): locks weather task for live enrichment    APACHE NIFI
    Highlight NiFi Processor    Fetch Open-Meteo FMI Weather
    Show Step    Queries Open-Meteo Nordic weather model for temperature, wind and daylight hours    APACHE NIFI
    Highlight NiFi Processor    Fetch Wikipedia Profile
    Show Step    Queries Wikipedia REST API for destination extract and scenic thumbnail photo    APACHE NIFI
    Highlight NiFi Processor    Complete Weather & Dossier Task
    Show Step    Completes task with live weather dossier; token advances to user curation in Tasklist    APACHE NIFI
    Subtle Cursor Glide    1.0
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
    Go To    ${NIFI_URL}/#/process-groups/${pg_id}
    Focus    actor    solo=${True}
    Wait Until Visible    \#canvas
    Show Step    Human curation approved: NiFi resumes execution for itinerary compilation & dispatch    APACHE NIFI
    Highlight NiFi Processor    Fetch Itinerary Compile Task
    Show Step    Column 3 (fin-itinerary-compile): locks task and extracts traveler choices (mökki stay, campfire cooking)    APACHE NIFI
    Highlight NiFi Processor    Map Regional Food & Gear
    Show Step    Maps regional Karelian delicacies (Karjalanpiirakat, paistetut muikut) and gear list    APACHE NIFI
    Highlight NiFi Processor    Render Adventure Markdown
    Show Step    Renders customized Markdown itinerary and returns payload to Operaton token    APACHE NIFI
    Highlight NiFi Processor    Complete Itinerary Compile
    Highlight NiFi Processor    Fetch Dispatch Task
    Show Step    Column 4 (fin-itinerary-dispatch): locks dispatch task and sets guide as FlowFile body    APACHE NIFI
    Highlight NiFi Processor    Deliver Itinerary to Mailpit
    Show Step    PutEmail dispatches personalized HTML/Markdown email to Mailpit SMTP (127.0.0.1:1025)    APACHE NIFI
    Highlight NiFi Processor    Archive Itinerary Markdown
    Highlight NiFi Processor    Complete Dispatch Task
    Show Step    PutFile archives guide to disk; CompleteExternalTask finishes the workflow    APACHE NIFI
    Subtle Cursor Glide    1.0
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
