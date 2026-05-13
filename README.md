# Event Analytics Dashboard

An experimental computational audience analytics and event intelligence platform designed for independent music and comedy venues.

This project combines software engineering, graph theory, optimisation algorithms, statistical analysis, and audience intelligence techniques to analyse ticket buyer behaviour, event relationships, attendance patterns, and venue performance.

The system uses The One Room Music & Comedy Club as a real-world case study for exploring how computational analytics can support independent creative venues.

---

# Project Background

This project originally began as a practical internal tool developed to simplify the extraction and organisation of customer phone numbers and email addresses for marketing and audience communication purposes.

While working at The One Room Music & Comedy Club between 2023 and March 2026 in:

* event operations
* artist relations
* media coordination
* audience engagement
* marketing support

I became increasingly involved in discussions around:

* audience growth
* event performance
* customer retention
* marketing effectiveness
* venue strategy

As operational responsibilities expanded, the project evolved from a simple contact extraction utility into a broader audience analytics and computational event intelligence system designed to support more informed decision-making during programming, marketing, and strategic planning meetings.

The platform gradually developed into a system intended to help answer questions such as:

* Which events attract repeat audiences?
* Which shows create crossover between audience communities?
* Which event categories generate the strongest retention?
* Which audiences are most engaged over time?
* How interconnected are audience groups across different genres?
* Which shows act as bridges between separate audience communities?
* Which combinations of events maximise audience reach?
* What attendance patterns emerge from ticketing data?

This repository represents the ongoing evolution of that system into a more advanced computational analytics and software engineering project.

---

# Learning and Development Context

This project was built while I was still actively learning:

* software engineering
* data structures and algorithms
* graph theory
* computational analytics
* optimisation techniques
* statistical computing
* large-scale system design concepts

Many parts of the system were developed iteratively through:

* experimentation
* operational necessity
* independent research
* rapid prototyping
* algorithm exploration
* gradual architectural refinement

As a result, portions of the codebase reflect:

* exploratory development
* evolving coding practices
* experimentation with computational methods
* iterative software design improvements

The project should therefore be viewed both as:

* a functional operational analytics platform
  and
* an ongoing software engineering and analytics learning project

Future development aims to improve:

* maintainability
* modularity
* scalability
* software architecture
* database integration
* code quality
* testing practices
* algorithm efficiency
* separation of concerns
* engineering best practices

The repository intentionally documents both:

* the current capabilities of the system
  and
* the progression of my technical development over time.

---

# Current Development Philosophy

The current implementation prioritises:

* experimentation
* operational usefulness
* analytics exploration
* algorithmic analysis
* rapid prototyping

within a real venue environment.

The long-term goal is to transition the platform from an operational analytics tool into a scalable computational audience intelligence system capable of handling:

* larger datasets
* richer analytics workflows
* database-backed processing
* advanced visualisation
* future AI and machine learning integrations

---

# Technologies Used

| Technology                        | Purpose                               |
| --------------------------------- | ------------------------------------- |
| Java                              | Core application development          |
| JavaFX                            | User interface development            |
| CSV Processing                    | Data ingestion and parsing            |
| ArrayList / Collections Framework | Data storage and manipulation         |
| Graph Structures                  | Audience-event relationship modelling |
| Dynamic Programming               | Optimisation algorithms               |
| Statistical Analysis              | Audience and event analytics          |
| Git & GitHub                      | Version control                       |
| IntelliJ IDEA                     | Development environment               |

---

# Core Computational Features

The system includes:

* audience relationship modelling
* event similarity analysis
* audience overlap detection
* community detection
* optimisation algorithms
* statistical inference
* demographic estimation
* retention analysis
* graph-based event intelligence
* ticketing analytics
* event network analysis

---

# Algorithms and Computational Methods Used

## Graph Theory Algorithms

### Bipartite Graph Construction

The platform models relationships between:

* audience members
* live events

using bipartite graph structures.

This enables:

* audience overlap analysis
* event relationship modelling
* network-based audience analysis

---

### Jaccard Similarity Analysis

The system computes audience overlap between events using:

J(A,B)=|A∩B| / |A∪B|

This identifies:

* highly related events
* crossover audiences
* event similarity structures

---

### Breadth-First Search (BFS)

Breadth-First Search is used for:

* connected audience community detection
* graph traversal
* relationship exploration within event-attendance networks

---

### Degree Centrality Analysis

The system computes event centrality based on:

* attendee connectivity
* audience reach
* event influence within the network

---

### Bridgeness Analysis

The platform estimates which events function as bridges between otherwise separate audience groups.

This helps identify:

* crossover events
* strategically important performances
* community-linking shows

---

## Statistical Algorithms

### Pearson Correlation Coefficient

Correlation analysis is performed between:

* ticket volume
* attendance
* unique audience counts

This is used to measure relationships between event metrics and identify meaningful attendance trends.

---

### Chi-Squared Statistical Testing

The platform performs chi-squared analysis on:

* booking time distributions
* attendance patterns

to detect statistically significant deviations from expected behaviour.

---

### Moving Average Analysis

Moving averages are used for:

* revenue smoothing
* trend analysis
* temporal event performance monitoring

---

### Z-Score Outlier Detection

The system detects unusually high or low performing events using statistical outlier analysis.

This identifies:

* anomalous ticket sales
* unusually successful shows
* underperforming events

---

### Cohort Retention Analysis

The platform analyses:

* repeat attendance
* audience retention
* customer loyalty patterns

across multiple events.

---

## Greedy Algorithms

### Greedy Set Cover Approximation

The project implements a greedy approximation algorithm to determine:

> the minimum number of events required to reach the largest possible audience coverage

This supports:

* marketing optimisation
* event scheduling strategy
* audience reach planning

---

### Category Reach Optimisation

Greedy optimisation techniques are used to determine:

* which event categories maximise marginal audience expansion

when selected sequentially.

---

## Dynamic Programming Algorithms

### 0/1 Knapsack Optimisation

The platform implements dynamic programming for event-selection optimisation using the classical knapsack problem.

The algorithm maximises:

* projected audience reach

subject to:

* limited scheduling constraints

---

### Longest Common Subsequence (LCS)

The system uses Longest Common Subsequence analysis to compare:

* attendee sequence similarity between events

This helps identify:

* deeply related audience groups
* recurring behavioural patterns
* event affinity structures

---

# Demographic Inference Systems

The platform experiments with probabilistic demographic estimation using:

* first-name analysis
* surname pattern analysis
* linguistic heuristics
* regional naming structures

These estimates are treated as:

* probabilistic
* exploratory
* non-deterministic

rather than exact demographic classifications.

---

# Data Structures Used

| Data Structure             | Purpose                          |
| -------------------------- | -------------------------------- |
| ArrayList                  | Dynamic data storage             |
| HashSet                    | Fast uniqueness detection        |
| LinkedHashMap              | Ordered analytics mapping        |
| LinkedHashSet              | Ordered unique collections       |
| Queue / LinkedList         | BFS traversal                    |
| 2D Arrays                  | Dynamic programming tables       |
| Graph adjacency structures | Audience-event network modelling |

---

# Software Engineering Concepts Demonstrated

This project demonstrates:

* Object-Oriented Programming (OOP)
* Event-driven programming
* Graph modelling
* Dynamic programming
* Statistical computing
* Computational analytics
* Graph traversal algorithms
* Algorithmic optimisation
* Data processing pipelines
* Separation of concerns
* Layered architecture principles

---

# Planned Engineering Improvements

Future development will focus on:

* database integration
* modular architecture
* scalable data pipelines
* improved maintainability
* cleaner service-layer abstraction
* improved separation of concerns
* API integration
* cloud deployment
* distributed analytics processing
* performance optimisation

---

# Planned Future Features

## Database Layer

* SQLite integration
* PostgreSQL integration
* scalable relational data storage

## Data Visualisation

* JavaFX charts
* interactive dashboards
* audience network visualisations
* revenue analytics dashboards

## AI and Machine Learning

* audience clustering
* recommendation systems
* attendance forecasting
* ticket demand prediction
* behavioural modelling
* predictive event analytics

---

# Why This Project Matters

Independent creative venues often lack access to advanced audience analytics systems available to larger entertainment organisations.

This project explores how:

* software engineering
* graph theory
* optimisation algorithms
* statistical analysis
* computational analytics

can support:

* programming decisions
* audience development
* strategic planning
* marketing effectiveness
* operational intelligence

within independent live entertainment environments.

---

# Long-Term Vision

The long-term goal is to evolve this platform into a scalable audience intelligence and event analytics system capable of supporting:

* independent venues
* festivals
* promoters
* live performance ecosystems
* audience research initiatives

through computational analytics and data-driven decision support systems.

---

# Repository Structure

```text
event-analytics-dashboard/
│
├── one-room-analytics-system/
│   ├── src/
│   ├── data/
│   ├── analytics/
│   └── ui/
│
├── screenshots/
├── README.md
└── .gitignore
```

---

# Author

Masego Madisha

BCom Computer Science and Information Systems student with a multidisciplinary background in:

* psychology
* audience engagement
* artist relations
* marketing coordination
* live entertainment operations
* computational analytics

Current areas of interest include:

* software engineering
* data science
* graph analytics
* machine learning
* business intelligence
* computational social analysis
* entertainment technology systems
