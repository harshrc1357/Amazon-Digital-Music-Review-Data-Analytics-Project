# Amazon Digital Music Review Data Analytics Project

This comprehensive data analytics project analyzes Amazon's Digital Music review dataset containing **130.4K reviews** from **101K users** across **70.5K items**. The project implements a sophisticated data pipeline combining multiple AWS services and distributed computing frameworks.

---

## Architecture Components

### Cloud Infrastructure
- **AWS S3** for scalable data storage
- **PySpark** for distributed data processing
- **AWS SageMaker** for machine learning capabilities
- **QuickSight** for data visualization

### Data Processing Pipeline
- Ingested raw JSON data from S3 containing customer reviews, ratings, and metadata
- Transformed timestamps into year/month format for temporal analysis
- Performed aggregations to analyze rating distributions, helpful votes, and review patterns
- Implemented machine learning models using SageMaker AutoPilot

---

## Key Findings

### Review Analysis
- Identified strong bias toward **5-star ratings** with **77.17% precision**
- Discovered **verified purchases** consistently rate higher than non-verified ones
- Found **helpful votes** increase proportionally with rating scores

### Machine Learning Results
- Achieved **77.24% overall accuracy**
- Revealed significant **class imbalance issues**
- Model showed limited generalization across lower rating categories

---

## Technical Implementation
- Utilized **Python with PySpark** for data processing, implementing comprehensive data transformation pipelines and statistical analyses
- Employed **AWS SageMaker AutoPilot** for predictive modeling
- Used **QuickSight** for interactive visualization of trends and patterns

---

## Impact
The analysis provided valuable insights into:
- Customer behavior and review patterns in the digital music marketplace
- Areas for potential improvement in review systems
- The relationship between verification status and review credibility
