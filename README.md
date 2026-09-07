\# EventIntelligence



EventIntelligence is a Java-based event and audience analytics application designed to transform event ticketing and attendee data into actionable operational and audience intelligence.



The project began as an analytics system for understanding event attendance and customer behaviour and is evolving into a broader event intelligence platform.



Rather than treating event records as isolated transactions, EventIntelligence analyses relationships between attendees, shows, event categories and purchasing behaviour to answer questions such as:



\- Which events share the same audiences?

\- Which attendees return to multiple events?

\- Which event categories attract overlapping audiences?

\- Which events act as bridges between different audience groups?

\- What combination of events provides the greatest audience reach?

\- How does audience behaviour change over time?

\- Which attendance or booking patterns appear unusual?

\- How effectively is an event portfolio retaining its audience?



\## Current Application



The current implementation is a Java application centred around:



```text

src/

├── Main.java

└── TicketBuyerWebApp.java

```



`Main.java` provides the application entry point.



`TicketBuyerWebApp.java` currently contains the core ingestion, processing, analytics, reporting and web application logic.



The project is being progressively refactored from this working implementation into a more modular architecture.



\## Core Capabilities



\### Audience Analytics



EventIntelligence processes event attendance data to build a consolidated view of audience behaviour across multiple shows.



Current analysis includes:



\- unique audience identification

\- repeat-attendee analysis

\- returning vs new audience analysis

\- event attendance counts

\- category-level audience analysis

\- ticket-volume analysis

\- average shows attended per customer

\- audience retention analysis

\- cross-event audience behaviour



\### Event and Category Analysis



Events can be grouped into categories such as comedy, jazz, hip-hop and Afro-soul.



The application analyses:



\- audience size by event

\- audience size by category

\- event/category relationships

\- shared audiences

\- cross-category attendance

\- event portfolio reach



\## Graph Analysis



EventIntelligence models relationships between audiences and events as a graph.



This enables analysis of relationships that are difficult to identify using simple aggregation alone.



Current graph-oriented analysis includes:



\- contact-to-show relationships

\- show-to-show audience overlap

\- connected components

\- degree-based analysis

\- Jaccard similarity

\- cross-genre audience identification

\- event bridgeness analysis



\### Jaccard Similarity



Audience similarity between two events can be represented as:



```text

J(A, B) = |A ∩ B| / |A ∪ B|

```



where `A` and `B` represent the audiences of two events.



A higher value indicates stronger audience overlap.



\## Statistical Analysis



The application includes statistical techniques for identifying patterns in event and audience data.



Implemented techniques include:



\### Pearson Correlation



Used to examine relationships between numerical variables in the dataset.



\### Chi-Square Analysis



Used to analyse whether observed audience or booking distributions differ meaningfully from expected distributions.



\### Moving Averages



Used to smooth sequential event data and identify broader trends.



\### Z-Score Outlier Detection



Used to identify observations that differ substantially from the surrounding distribution.



\### Cohort and Retention Analysis



Used to examine how effectively audiences return after their initial interaction with the event portfolio.



\## Optimisation Algorithms



EventIntelligence goes beyond descriptive reporting by applying algorithms to event-management questions.



\### Greedy Set-Cover Approximation



A greedy optimisation strategy is used to determine an efficient sequence of events for reaching the largest possible portion of the known audience.



Conceptually:



```text

while uncovered audience remains:

&#x20;   select the event covering the most currently uncovered people

&#x20;   add the event to the solution

&#x20;   mark those people as covered

```



This can support questions such as:



> If only a limited number of events could be promoted, which events collectively reach the broadest audience?



\### Dynamic Programming



The application contains dynamic-programming approaches for optimisation problems including:



\- 0/1 Knapsack

\- Longest Common Subsequence (LCS)



Knapsack-style optimisation can be applied where event selections must be made under a limited resource or scheduling budget.



LCS provides another mechanism for examining similarity between sequences.



\## Data Processing



The application imports event data from CSV files and performs processing before analytics are calculated.



Processing includes:



\- CSV parsing

\- attendee extraction

\- duplicate handling

\- email normalisation

\- telephone-number processing

\- exclusion handling

\- event categorisation

\- audience aggregation

\- show-level aggregation

\- category-level aggregation



\## Data Privacy



The original application operates on private event and attendee information.



\*\*Real attendee data is not stored in this public repository.\*\*



The local directory:



```text

app\_data/

```



is explicitly excluded from Git through `.gitignore`.



This protects information such as:



\- customer email addresses

\- telephone numbers

\- booking records

\- attendance records

\- private event exports



A synthetic demonstration dataset will be provided separately so that the public project can demonstrate the analytics pipeline without exposing real customer information.



\## Project Structure



```text

EventIntelligence/

│

├── src/

│   ├── Main.java

│   └── TicketBuyerWebApp.java

│

├── app\_data/

│   └── private local data (not committed)

│

├── .gitignore

└── README.md

```



\## Technology



The current implementation primarily uses:



\- Java

\- Java Collections Framework

\- Java HTTP Server

\- CSV-based data ingestion

\- HTML/CSS/JavaScript generated by the Java application

\- graph algorithms

\- statistical analysis

\- greedy algorithms

\- dynamic programming



The application intentionally demonstrates the underlying data-processing and algorithmic logic rather than relying entirely on external analytics libraries.



\## Analytics Pipeline



At a high level, EventIntelligence follows the following pipeline:



```text

Event CSV Data

&#x20;     |

&#x20;     v

Data Ingestion

&#x20;     |

&#x20;     v

Cleaning \& Normalisation

&#x20;     |

&#x20;     v

Audience / Event Aggregation

&#x20;     |

&#x20;     +-------------------+

&#x20;     |                   |

&#x20;     v                   v

Statistical Analysis   Graph Analysis

&#x20;     |                   |

&#x20;     +---------+---------+

&#x20;               |

&#x20;               v

&#x20;       Optimisation

&#x20;               |

&#x20;               v

&#x20;     Audience Intelligence

&#x20;               |

&#x20;               v

&#x20;      Reports / Dashboard

```



\## Engineering Direction



The current code represents a working prototype and analytical foundation.



The next stage of development focuses on separating the monolithic application into clearly defined components such as:



```text

ingestion

domain

analytics

services

controllers

reporting

```



This will make the application easier to test, extend and maintain while preserving the algorithms and analytical behaviour of the original implementation.



Future development will also introduce a synthetic demonstration dataset so that the complete analytics workflow can be reproduced publicly.



\## Planned Development



The roadmap includes:



\- modularising the current Java application

\- separating ingestion from analytics

\- introducing domain models

\- improving automated testing

\- generating synthetic demonstration data

\- improving analytics visualisation

\- expanding event portfolio analysis

\- developing richer audience segmentation

\- introducing persistent storage

\- exposing analytics through cleaner APIs

\- expanding forecasting and optimisation capabilities



\## Why EventIntelligence?



Traditional event reporting often stops at metrics such as:



```text

Tickets Sold

Revenue

Attendance

```



EventIntelligence aims to answer a deeper set of questions:



```text

Who is the audience?

How often do they return?

Which events share audiences?

Which categories attract the same people?

Which events connect otherwise separate audience groups?

What patterns exist across the portfolio?

How can limited resources be allocated more effectively?

```



The goal is to move from \*\*event reporting\*\* toward \*\*event intelligence\*\*.



\---



\*\*Status:\*\* Active development

