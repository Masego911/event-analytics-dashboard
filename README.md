# event-analytics-dashboard
# One Room Analytics System

## Overview
This is a full-stack Java-based analytics system built to analyse event performance, audience behaviour, and revenue trends for a live events business.

The system processes raw CSV ticket data and transforms it into actionable insights through a web-based dashboard.

## Features

### Data Processing
- CSV ingestion and parsing
- Data cleaning and validation
- Deduplication of contacts
- Category classification (Afrosoul, Jazz, Comedy, etc.)

### Analytics Engine
- Revenue tracking (total, per category, per year)
- Customer segmentation (new vs returning)
- Attendance rate calculation
- Peak booking time detection
- Group size analysis
- Seasonal performance trends

### Dashboard
- Web-based dashboard built using Java HttpServer
- Interactive charts using Chart.js:
  - Revenue over time
  - Revenue by category
  - Audience mix
  - Time behaviour
  - Group behaviour
  - Seasonality

### Data Management
- Upload new CSV files
- Download processed datasets
- Exclude specific contacts
- Override category classification

## Key Results (Real Data)
- Revenue increased from R58,100 → R583,070 (+903.6%)
- Total analysed revenue: R641,170
- Peak booking time: Evening
- Top category: Afrosoul
- Returning audience: 24.1%

## Tech Stack
- Java (Core + HttpServer)
- HTML/CSS (generated)
- Chart.js
- CSV data processing

## How to Run

1. Compile:
2. Run
3. Open Browser:


## What I Learned
- Building backend systems without frameworks
- Handling real-world messy data
- Designing analytics pipelines
- Turning data into business insights

## Future Improvements
- Move to Spring Boot
- Add database (PostgreSQL)
- Build React frontend
- Add predictive analytics
