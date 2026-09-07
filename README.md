# EventIntelligence

> A data analysis and audience intelligence tool built from real-world live event data.

![Java](https://img.shields.io/badge/Java-Current-orange)
![Data Analysis](https://img.shields.io/badge/Data-Analysis-blue)
![Spring Boot](https://img.shields.io/badge/Spring_Boot-Planned-success)
![SQL](https://img.shields.io/badge/SQL_Database-Planned-red)
![Python](https://img.shields.io/badge/Python-Planned-3776AB)
![Pandas](https://img.shields.io/badge/Pandas-Planned-150458)
![Power BI](https://img.shields.io/badge/Power_BI-Planned-F2C811)
![React](https://img.shields.io/badge/React-Planned-61DAFB)

---

## What is EventIntelligence?

**EventIntelligence is a data analysis project built around real-world event and audience data from South Africa's live entertainment industry.**

The project started with a practical problem I encountered while working in live event operations.

Ticketing platforms could tell us how many tickets had been sold for an individual event, but I regularly needed answers that went beyond individual ticket sales:

- Who are our customers?
- Which customers keep returning?
- Which shows share the same audiences?
- Which genres have overlapping audiences?
- Which customers attend multiple types of events?
- Which events bring new people into the venue?
- Which audiences should be targeted for a particular campaign?
- How is our audience changing over time?
- Which events contribute most to audience retention?
- What does the data tell us about the performance of the business?

The information required to answer these questions existed, but much of it was distributed across separate CSV exports from individual events.

EventIntelligence grew from an attempt to make that data useful.

It is therefore not simply a dashboard project.

It is an evolving **data analysis, software engineering and decision-support project** built around a real operational dataset and a real business problem.

---

# The Story Behind the Project

I worked in event and artist relations at **The One Room Music & Comedy Club**, an independent live entertainment venue in Gqeberha, South Africa.

My work involved event operations, artist relations, audience engagement, marketing, media coordination and reporting.

Part of that work involved dealing with ticketing data.

Each event generated its own customer records.

Over time, this meant working with data from many different events including:

- jazz
- comedy
- Afro-soul
- hip-hop
- live music
- mixed-format events

Initially, my problem was simple.

I needed a faster way to extract customer email addresses and telephone numbers from multiple ticketing exports.

The manual process involved repeatedly opening CSV files, identifying customers, removing duplicates and creating communication lists.

So I built a Java program to automate that process.

But once the data from different events could be processed together, a much more interesting question emerged:

> **What else can this data tell us?**

That question changed the direction of the project.

---

# From Contact Extraction to Data Analysis

The original utility began evolving into an audience analytics application.

Instead of only asking:

> "Give me the email addresses from this event."

I could begin asking:

> "How many of these people have attended before?"

Then:

> "What other events did they attend?"

Then:

> "Which events have similar audiences?"

Then:

> "Which genres share audiences?"

And eventually:

> "What patterns exist across the entire event portfolio?"

This is how EventIntelligence developed.

The project now explores how raw operational event data can be transformed into useful information through:

- data cleaning
- data transformation
- aggregation
- descriptive analytics
- customer segmentation
- retention analysis
- statistical analysis
- graph analysis
- algorithmic optimisation
- business intelligence

---

# The Data

The project was developed using **real operational event data**.

The source data consists primarily of CSV exports associated with individual events.

These records contain information required to analyse areas such as:

- event attendance
- customer participation
- booking behaviour
- ticket quantities
- customer contact information
- event categories
- repeat attendance
- audience relationships

Because this repository is public, the original operational data is **not included**.

The real dataset is stored locally under:

```text
app_data/
```

and is explicitly excluded through `.gitignore`.

This protects private customer and operational information.

A synthetic demonstration dataset will be introduced later so that the complete analytical workflow can be demonstrated publicly without exposing real customer data.

---

# Current Project

The current implementation is a working **Java prototype**.

```text
EventIntelligence/
│
├── src/
│   ├── Main.java
│   └── TicketBuyerWebApp.java
│
├── app_data/
│   └── private operational data
│
├── .gitignore
└── README.md
```

The current application uses:

- Java
- Java Collections Framework
- CSV files
- Java's embedded HTTP server
- server-generated HTML/CSS/JavaScript
- browser-based analytical views

Most of the application currently resides in `TicketBuyerWebApp.java`.

This is intentional historical context rather than the desired final architecture.

The program grew organically as I added new analytical questions and features.

One of the next major engineering objectives is therefore to break this monolithic implementation into properly separated application components.

---

# Current Data Pipeline

The existing application effectively performs a small data-processing pipeline.

```text
Real Event CSV Files
        |
        v
   Data Ingestion
        |
        v
 Cleaning & Validation
        |
        v
   Normalisation
        |
        v
 Deduplication
        |
        v
 Event / Customer Mapping
        |
        v
 Analytical Processing
        |
        +--------------------+
        |                    |
        v                    v
Descriptive Analysis   Computational Analysis
        |                    |
        +----------+---------+
                   |
                   v
          Audience Intelligence
                   |
                   v
          Browser Dashboard
```

This pipeline is one of the reasons I now consider EventIntelligence primarily a **data analysis system supported by software engineering**, rather than simply an event-management application.

---

# Data Cleaning and Preparation

Before meaningful analysis can take place, the application has to deal with real-world data quality problems.

The Java implementation performs processing including:

- reading multiple CSV files
- combining event records
- extracting customer information
- normalising email addresses
- processing telephone numbers
- removing duplicate records
- handling exclusions
- assigning event categories
- creating event/customer relationships
- aggregating attendance information

This part of the project has been particularly important because real operational data is rarely analysis-ready.

A significant lesson from the project has been that useful analytics depends on reliable data preparation.

---

# Current Analytics

## Descriptive Analytics

The application calculates information such as:

- unique customers
- event attendance
- repeat attendance
- customer participation frequency
- event-level audience size
- category-level audience size
- ticket quantities
- returning audiences
- audience retention

These metrics help answer:

> **What happened?**

---

## Audience Analysis

EventIntelligence creates a consolidated view of customer behaviour across multiple events.

This makes it possible to analyse:

- once-off customers
- repeat customers
- highly engaged customers
- customers attending multiple events
- cross-category customers
- returning audiences
- customer attendance histories

Instead of treating every ticket purchase as an isolated transaction, the application attempts to understand the **customer relationship across time and events**.

---

# Graph Analytics

Some of the most interesting questions in the dataset concern relationships.

A customer can attend many events.

An event can have many customers.

This naturally forms a graph.

EventIntelligence uses graph-oriented analysis to explore these relationships.

Current implementations include:

- customer-to-event relationships
- event-to-event audience relationships
- connected components
- Breadth-First Search
- degree-based analysis
- event similarity
- audience crossover
- cross-genre audiences
- bridgeness analysis

---

## Event Similarity

Two events can be compared according to how much of their audience they share.

The application uses **Jaccard similarity**:

```text
J(A,B) = |A ∩ B| / |A ∪ B|
```

where:

- `A` = audience of event A
- `B` = audience of event B

This provides a way of asking:

> Which events attract similar groups of people?

This has practical applications for programming, customer segmentation and marketing.

---

# Statistical Analysis

The Java prototype also contains statistical analysis methods.

Current implementations include:

### Pearson Correlation

Used to examine relationships between numerical variables.

### Chi-Square Analysis

Used to investigate distributions within event and booking data.

### Moving Averages

Used to examine changes and trends across sequential observations.

### Z-Score Outlier Detection

Used to identify unusually high or low observations.

### Cohort / Retention Analysis

Used to examine whether audiences return after their initial interaction with the event portfolio.

These methods move the project beyond basic totals and counts toward **diagnostic and exploratory data analysis**.

---

# Optimisation Algorithms

The project also explores how algorithms can support decision-making.

## Greedy Set Cover

A greedy set-cover approximation is used to explore audience reach.

The underlying question is:

> If I can only select a limited number of events or audience groups, which selections collectively give me the greatest reach?

This has potential applications in:

- marketing
- campaign planning
- audience development
- event programming

---

## Dynamic Programming

The application contains implementations of:

- 0/1 Knapsack
- Longest Common Subsequence

These were introduced while exploring how computer science algorithms could be connected to genuine event-management and audience-analysis problems rather than existing only as classroom exercises.

---

# Why This Is a Data Analysis Project

EventIntelligence follows the basic lifecycle of a data analysis problem:

```text
Business Problem
       |
       v
Data Collection
       |
       v
Data Cleaning
       |
       v
Data Transformation
       |
       v
Exploratory Analysis
       |
       v
Descriptive Analytics
       |
       v
Diagnostic Analytics
       |
       v
Visualisation
       |
       v
Business Insight
       |
       v
Decision Support
```

The software exists to support this analytical process.

The long-term objective is to develop the project into a system capable of supporting:

### Descriptive Analytics

**What happened?**

Examples:

- attendance
- ticket sales
- audience size
- repeat attendance

### Diagnostic Analytics

**Why did it happen?**

Examples:

- event comparisons
- audience overlap
- retention patterns
- category relationships

### Predictive Analytics

**What is likely to happen?**

Future examples:

- attendance forecasting
- customer churn
- demand forecasting
- revenue forecasting

### Prescriptive Analytics

**What should we do?**

Future examples:

- campaign targeting
- event recommendations
- audience selection
- marketing optimisation

---

# Why the Project Is Being Rebuilt

The existing Java application works, but its architecture reflects how the project developed.

It started as a small operational utility.

New requirements were added as new questions emerged.

Eventually, ingestion, analytics, HTTP handling, reporting and presentation logic accumulated inside a large Java application.

That has created a valuable engineering problem of its own:

> How do I take a working prototype and redesign it as a maintainable data and software platform?

The next phase of EventIntelligence addresses exactly that problem.

I am rebuilding the project incrementally rather than discarding the original implementation.

The existing Java prototype provides the **baseline behaviour and analytical requirements** for the new system.

---

# Modernisation Roadmap

The technologies below represent the **planned evolution** of EventIntelligence.

They are not presented as technologies already implemented in the current prototype.

---

## Phase 1 — Refactor and Requirements Analysis

Before changing frameworks, the current application will be analysed and documented.

This includes:

- identifying business requirements
- documenting current functionality
- identifying analytical requirements
- separating business rules from implementation details
- defining entities and relationships
- documenting data quality rules
- defining measurable KPIs
- identifying technical debt

This creates a clear baseline for the rebuild.

---

# Phase 2 — Spring Boot Backend

The Java application will be migrated to **Spring Boot**.

The purpose of introducing Spring Boot is not simply to add another framework.

The goal is to separate responsibilities currently concentrated in the prototype.

The planned backend will introduce components such as:

```text
controller/
service/
repository/
model/
dto/
analytics/
ingestion/
validation/
exception/
config/
```

The Spring Boot application will expose REST APIs that can be consumed independently by analytical and presentation tools.

This phase will also introduce:

- dependency injection
- validation
- exception handling
- service boundaries
- API design
- automated testing
- configuration management

---

# Phase 3 — Relational Database

CSV files are useful as source data but are not an appropriate long-term operational data store.

The next architecture will therefore introduce a relational database.

The database design will model concepts such as:

```text
Customer
Event
EventCategory
Booking
Ticket
Attendance
Venue
```

The work will include:

- requirements analysis
- conceptual data modelling
- ERD development
- normalisation
- primary and foreign keys
- constraints
- indexes
- SQL queries
- views
- transactions
- data migration

The objective is to create a reliable analytical foundation rather than simply transferring CSV files into database tables.

---

# Phase 4 — Data Engineering Pipeline

The existing Java CSV-processing logic will evolve into a more explicit data pipeline.

Conceptually:

```text
Raw Event Data
      |
      v
    BRONZE
Original source data
      |
      v
    SILVER
Cleaned and standardised data
      |
      v
     GOLD
Analysis-ready datasets
```

The pipeline will focus on:

- ingestion
- validation
- cleaning
- standardisation
- deduplication
- transformation
- enrichment
- quality checks
- reproducibility

This will allow the same analytical process to be repeated as new event data becomes available.

---

# Phase 5 — Python and Pandas

Once the operational data model and ingestion pipeline are established, **Python** will become an additional analytical layer.

Python will not replace the Java backend.

Instead, the two technologies will serve different purposes.

### Java / Spring Boot

Responsible for:

- application logic
- APIs
- domain rules
- data access
- operational workflows

### Python

Responsible for:

- exploratory data analysis
- statistical analysis
- analytical experimentation
- modelling
- data science workflows

### Pandas

Pandas will be used for:

- dataframe manipulation
- data cleaning
- grouping
- aggregation
- joining datasets
- exploratory analysis
- feature preparation
- analytical validation

This also creates an opportunity to compare analytical results from the existing Java implementation with equivalent Python/Pandas analysis.

---

# Phase 6 — Power BI

**Power BI** will provide the business intelligence layer.

Instead of embedding every visualisation directly inside application code, curated datasets will be exposed to Power BI.

Planned dashboards include:

### Executive Dashboard

- total events
- attendance
- revenue
- customer growth
- repeat attendance
- retention

### Audience Dashboard

- unique customers
- new vs returning customers
- attendance frequency
- customer segments
- cross-category audiences

### Event Performance Dashboard

- event comparisons
- category performance
- attendance trends
- revenue trends
- audience retention

### Marketing Dashboard

- audience reach
- customer segments
- campaign populations
- inactive customers
- cross-sell opportunities

Power BI will therefore answer the business intelligence side of the project while Python and Java provide the underlying processing and analytical capabilities.

---

# Phase 7 — React Frontend

A **React** frontend is planned for operational users who need to interact directly with EventIntelligence.

React will consume the Spring Boot REST API.

Potential functionality includes:

- customer search
- event search
- event analytics
- audience exploration
- communication-list generation
- data imports
- analytical reports
- filters
- interactive charts
- administrative functionality

The distinction is important:

**Power BI** will primarily support analytical reporting and business intelligence.

**React** will support application workflows and interactive operational functionality.

---

# Target Architecture

The planned architecture is:

```text
                 Ticketing / Event Data
                          |
                          v
                   Data Ingestion
                          |
                          v
              Cleaning & Transformation
                          |
                          v
                 Relational Database
                          |
             +------------+-------------+
             |                          |
             v                          v
      Spring Boot API             Python / Pandas
             |                          |
             |                    Data Analysis
             |                          |
             +------------+-------------+
                          |
                    Curated Data
                    /           \
                   v             v
              React App       Power BI
                   \             /
                    \           /
                     v         v
                    Decision Support
```

---

# Future Predictive Analytics

Machine learning will only be introduced after the descriptive and diagnostic analytical foundations are reliable.

Potential future questions include:

- Which customers are likely to stop attending?
- How many tickets is an event likely to sell?
- Which customers are most likely to attend a particular event?
- Which event categories are likely to perform well?
- What revenue can be expected from an upcoming event?
- Which audiences should receive a campaign?

Potential techniques include:

- regression
- classification
- clustering
- recommendation systems
- time-series forecasting

The objective is not to add AI for its own sake.

Predictive techniques will only be introduced where they answer a meaningful business question supported by sufficient data.

---

# What I Am Learning Through This Project

EventIntelligence is also a practical learning project.

Instead of learning technologies independently, I am using one real business problem to understand how different disciplines connect.

The project allows me to develop practical knowledge of:

- Java
- object-oriented programming
- algorithms and data structures
- SQL
- relational database design
- Spring Boot
- REST APIs
- data engineering
- Python
- Pandas
- statistical analysis
- exploratory data analysis
- Power BI
- business intelligence
- React
- software architecture
- testing
- Git and GitHub

More importantly, it requires understanding **why and where each technology belongs in a system**.

---

# Engineering Approach

The modernisation of EventIntelligence will follow an incremental Software Development Life Cycle.

```text
1. Understand the business problem
2. Analyse the existing prototype
3. Define requirements
4. Profile and understand the data
5. Design the domain model
6. Design the database
7. Refactor the backend
8. Build the data pipeline
9. Validate the analytics
10. Build BI dashboards
11. Build operational interfaces
12. Test
13. Deploy
14. Measure and improve
```

Each major architectural decision will be documented as the project evolves.

---

# Current vs Planned Technology

| Area | Current | Planned |
|---|---|---|
| Core application | Java | Java + Spring Boot |
| Source data | CSV | CSV ingestion + relational database |
| Data processing | Java | Java + Python |
| Data analysis | Java algorithms | Python + Pandas + Java analytics |
| Statistical analysis | Java implementation | Python analytical ecosystem |
| Backend | Embedded Java HTTP server | Spring Boot REST API |
| Operational UI | Browser-based HTML generated by Java | React |
| Business intelligence | Prototype analytical views | Power BI |
| Data persistence | Local CSV files | Relational database |
| Predictive analytics | Not implemented | Python / ML |
| Public data | No real customer data | Synthetic demonstration dataset |

---

# Data Privacy

The project was developed from genuine operational event data.

That creates a responsibility to protect the people represented in the dataset.

Real customer information is therefore **never intentionally committed to this public repository**.

The following are excluded:

```text
app_data/
contacts_with_shows.csv
excluded_emails.txt
.env
.env.*
```

The public version of the project will eventually contain synthetic data designed to reproduce the analytical characteristics needed to demonstrate the system without exposing identifiable customer information.

---

# Repository Status

**Current stage:** Working Java analytics prototype / architecture modernisation.

Current source:

```text
src/
├── Main.java
└── TicketBuyerWebApp.java
```

The large `TicketBuyerWebApp.java` file represents the historical growth of the prototype.

It is not presented as the desired final architecture.

The repository will document the process of transforming that prototype into a modular data analysis and decision-support platform.

---

# Project Direction

EventIntelligence began with a very small operational question:

> How can I stop manually extracting customer contact information from event CSV files?

That became:

> What can I learn when I combine the data from all these events?

And that has now become:

> How can software engineering, data engineering and data analysis transform operational event data into useful intelligence for real business decisions?

That progression is the foundation of the project.

The goal is not merely to build another event-management application.

The goal is to build a system that demonstrates the complete journey from:

**raw operational data → reliable information → analysis → insight → decision support.**

---

# Author

## Masego Madisha

BCom Computer Science and Information Systems

Interests:

- Software Engineering
- Data Engineering
- Data Analysis
- Business Intelligence
- Backend Development
- Applied Algorithms

EventIntelligence combines my software development studies with practical experience working with events, audiences, marketing and operational data in South Africa's live entertainment industry.
