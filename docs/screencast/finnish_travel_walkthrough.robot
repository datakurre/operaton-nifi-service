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

Walkthrough BPMN Process In Cockpit
    Start Observer    operaton    ${OPERATON_URL}/app/cockpit/default/#/login
    Chapter    eyebrow=Operaton BPMN & Apache NiFi 2.4.0 · Part 1
    ...    title=Finnish Travel & Outdoor Adventure Planner
    ...    subtitle=BPMN 2.0 orchestration model & boundary error handling
    Log In To Operaton    admin    admin
    Navigate To Process In Cockpit    finnish-travel-planner
    Show Step    Operaton Cockpit: BPMN 2.0 orchestration model for Finnish adventure planning    OPERATON
    Highlight Bpmn Activity    StartEvent_1
    Show Step    Start Event: initializes trip planning with 1-click presets or custom destination    OPERATON
    Trace Process Route In Cockpit    StartEvent_1    Task_LookupDestination
    Highlight Bpmn Activity    Task_LookupDestination
    Show Step    Service Task 1 (fin-trip-lookup): external task worker resolves coordinates via Open-Meteo    OPERATON
    Highlight Bpmn Activity    Boundary_UnknownDestination
    Show Step    Error Boundary: catches UNKNOWN_DESTINATION if geocoding fails or is outside Finland    OPERATON
    Trace Process Route In Cockpit    Boundary_UnknownDestination    Task_FallbackNotice
    Highlight Bpmn Activity    Task_FallbackNotice
    Show Step    Fallback Worker: delivers suggestion email with iconic Finnish destination alternatives    OPERATON
    Trace Process Route In Cockpit    Task_LookupDestination    Task_FetchWeather
    Highlight Bpmn Activity    Task_FetchWeather
    Show Step    Service Task 2 (fin-weather-check): enriches trip with live FMI Nordic weather & Wikipedia bio    OPERATON
    Trace Process Route In Cockpit    Task_FetchWeather    UserTask_CurateTrip
    Highlight Bpmn Activity    UserTask_CurateTrip
    Show Step    User Task: traveler reviews live weather and curates accommodation, sauna & dining    OPERATON
    Trace Process Route In Cockpit    UserTask_CurateTrip    Gateway_Confirm
    Highlight Bpmn Activity    Gateway_Confirm
    Trace Process Route In Cockpit    Gateway_Confirm    Task_CompileItinerary
    Highlight Bpmn Activity    Task_CompileItinerary
    Show Step    Service Task 3 (fin-itinerary-compile): maps regional gastronomy & compiles Markdown guide    OPERATON
    Trace Process Route In Cockpit    Task_CompileItinerary    Task_DispatchItinerary
    Highlight Bpmn Activity    Task_DispatchItinerary
    Highlight Bpmn Activity    EndEvent_Success
    Show Step    Service Task 4 (fin-itinerary-dispatch): PutEmail delivers to Mailpit; PutFile archives guide    OPERATON
    Subtle Cursor Glide    1.0

Launch Trip In Tasklist
    Go To    ${OPERATON_URL}/app/tasklist/default/#/
    Show Step    Operaton Tasklist: 1-click start form presets for iconic Finnish destinations    OPERATON
    Start Process In Tasklist    koli
    Show Step    Launched trip instance for Koli National Park — token waiting for NiFi workers    OPERATON
    Subtle Cursor Glide    1.0

Observe NiFi Pipeline
    [Setup]    Start Actor Turn    nifi    eyebrow=Operaton BPMN & Apache NiFi 2.4.0 · Part 2
    ...    title=Real-Time Geocoding & Live FMI Weather
    ...    subtitle=Zooming into Column 1 & Column 2: Open-Meteo & Wikimedia open APIs
    ...    anonymous=${True}
    ${pg_id}=    Get Travel Planner Group Id
    Go To    ${NIFI_URL}/#/process-groups/${pg_id}
    Focus    actor    solo=${True}
    Wait Until Visible    \#canvas
    Show Step    Apache NiFi 2.4.0: zooming into external task processors for real-time API orchestration    APACHE NIFI
    Zoom To NiFi Processor    Fetch Destination Task    0.85
    Show Step    Column 1: FetchAndLockExternalTasks acquires task lease from Operaton topic fin-trip-lookup    APACHE NIFI
    Zoom To NiFi Processor    Geocode Open-Meteo    0.85
    Show Step    Geocode Open-Meteo: calls public Open-Meteo API to resolve coordinates for Lieksa / Koli    APACHE NIFI
    Zoom To NiFi Processor    Validate Destination Location    0.85
    Show Step    RouteOnAttribute: verifies Finnish coordinates; unknown locations route to BPMN error    APACHE NIFI
    Zoom To NiFi Processor    Complete Destination Lookup    0.85
    Show Step    CompleteExternalTask: returns resolved lat (63.09) and lon (29.80) to Operaton token    APACHE NIFI
    Zoom To NiFi Processor    Fetch Weather Task    0.85
    Show Step    Column 2: FetchAndLockExternalTasks acquires fin-weather-check task for live enrichment    APACHE NIFI
    Zoom To NiFi Processor    Fetch Open-Meteo FMI Weather    0.85
    Show Step    Fetch Open-Meteo FMI Weather: queries Nordic weather model for temperature and wind    APACHE NIFI
    Zoom To NiFi Processor    Fetch Wikipedia Profile    0.85
    Show Step    Fetch Wikipedia Profile: extracts cultural bio and scenic photo thumbnail from Wikimedia    APACHE NIFI
    Zoom To NiFi Processor    Complete Weather & Dossier Task    0.85
    Show Step    CompleteExternalTask: delivers live weather dossier; token advances to user curation    APACHE NIFI
    Reset NiFi Zoom
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
    Zoom To NiFi Processor    Fetch Itinerary Compile Task    0.85
    Show Step    Column 3: locks fin-itinerary-compile task and extracts traveler choices (mökki, campfire)    APACHE NIFI
    Zoom To NiFi Processor    Map Regional Food & Gear    0.85
    Show Step    Map Regional Food & Gear: maps Karelian pasties, muikku dinner & outdoor equipment checklist    APACHE NIFI
    Zoom To NiFi Processor    Render Adventure Markdown    0.85
    Show Step    Render Adventure Markdown: generates personalized adventure guide from enriched attributes    APACHE NIFI
    Zoom To NiFi Processor    Complete Itinerary Compile    0.85
    Show Step    CompleteExternalTask: delivers compiled guide back to Operaton process instance    APACHE NIFI
    Zoom To NiFi Processor    Fetch Dispatch Task    0.85
    Show Step    Column 4: locks fin-itinerary-dispatch task and sets Markdown guide as FlowFile body    APACHE NIFI
    Zoom To NiFi Processor    Deliver Itinerary to Mailpit    0.85
    Show Step    PutEmail: dispatches personalized adventure briefing to Mailpit SMTP (127.0.0.1:1025)    APACHE NIFI
    Zoom To NiFi Processor    Archive Itinerary Markdown    0.85
    Show Step    PutFile: archives guide to deployment/itineraries; CompleteExternalTask concludes workflow    APACHE NIFI
    Reset NiFi Zoom
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
