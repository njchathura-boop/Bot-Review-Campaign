# Real-Time Fake Review and Bot Campaign Detection Using MLOps

> **Implementation status:** Phase 1 is now runnable. The included data is synthetic
> smoke-test data only; its metrics must not be reported as research results. See
> [the build plan](docs/BUILD_PLAN.md) for dataset, leakage, evaluation, deployment,
> governance, and phased-production decisions.

## Quick start

Use Python 3.11–3.14 from the project root:

```powershell
python -m pip install -e ".[dev]"
python -m pytest -q -p no:cacheprovider
python -m bot_campaign.cli validate data/sample/labeled_reviews.jsonl --labeled
python -m bot_campaign.cli train
python -m bot_campaign.cli campaigns
python -m uvicorn bot_campaign.api:app --reload
```

Open `http://127.0.0.1:8000` for the ecommerce trust-analysis demo and
`http://127.0.0.1:8000/docs` for the API. Replace the sample data only through the
canonical schema/adapters; do not train on Amazon Reviews as though it contains fake labels.

The UI now includes a visible eight-stage review scan, recent-review audit feed,
campaign replay and moderation console, deployment health, monitoring summaries, and
prediction lineage. Generate the controlled 50,000-event English scenario dataset with:

```powershell
python -m pip install -e ".[synthetic]"
python -m bot_campaign.cli generate-synthetic
```

See [operations and versioning](docs/OPERATIONS.md) for Docker Compose, monitoring,
Git/commit workflow, Kubernetes releases, and rollback.

For the Amazon Reviews 2023 → Kafka → Spark pipeline and Ray + MLflow training stack,
see [the distributed pipeline guide](docs/DISTRIBUTED_PIPELINE.md).

## Project Overview

This project focuses on building a real-time, production-ready MLOps pipeline for detecting fake reviews and coordinated bot-driven manipulation campaigns on online review platforms. The goal is not only to classify individual reviews as genuine or suspicious, but also to identify group-level campaign behavior using textual, behavioral, temporal, and similarity-based signals.

The system is designed as an end-to-end MLOps project covering data ingestion, data validation, feature engineering, model training, experiment tracking, model versioning, deployment, monitoring, drift detection, CI/CD, and responsible AI practices.

## Problem Statement

Online review platforms are increasingly affected by coordinated fake review campaigns, where groups of bot or low-credibility accounts post deceptive reviews to manipulate product ratings, reputation, and customer trust. Traditional fake review detection systems often rely only on static text classification, which is insufficient because modern campaigns are dynamic, coordinated, and adaptive.

These campaigns may use human-like language, AI-generated reviews, repeated semantic intent, burst posting patterns, unusual rating behavior, and distributed user accounts to avoid detection. Therefore, a robust detection system must analyze not only the content of reviews, but also the behavior of users, timing of activity, similarity between reviews, and campaign-level patterns.

This project aims to design and implement a real-time MLOps-based detection system that identifies both individual fake reviews and coordinated bot campaigns. The system will combine natural language processing, behavioral analytics, temporal anomaly detection, clustering, and production MLOps practices to create a scalable, reproducible, explainable, and monitorable machine learning pipeline.

## Research Motivation

Most basic fake review detection projects have the following limitations:

- They focus only on review text.
- They ignore user behavior and review timing.
- They do not detect coordinated campaigns.
- They are trained offline and not tested in real-time scenarios.
- They do not handle data drift or changing spam patterns.
- They lack data versioning, model versioning, and experiment tracking.
- They do not include deployment, monitoring, or CI/CD.

This project improves on those limitations by building a complete MLOps pipeline that supports both machine learning research and production-style deployment.

## Research Questions

1. How effectively can textual, behavioral, temporal, and similarity-based features improve fake review detection compared to text-only models?
2. Can coordinated bot campaigns be detected in real time using streaming review activity and clustering-based methods?
3. How does model performance change under data drift, such as new review styles, AI-generated text, or rating distribution changes?
4. How can MLOps practices such as data versioning, experiment tracking, monitoring, and CI/CD improve the reliability of fake review detection systems?
5. What is the trade-off between model accuracy, inference latency, and scalability in a real-time detection pipeline?

## Project Objectives

- Build a batch and streaming data ingestion pipeline for review data.
- Clean, validate, and version datasets using reproducible data engineering practices.
- Engineer textual, behavioral, temporal, and similarity-based features.
- Train and compare multiple machine learning and deep learning models.
- Detect individual fake reviews and coordinated suspicious campaigns.
- Track experiments, metrics, parameters, and artifacts using MLflow.
- Version datasets and models using DVC.
- Deploy the selected model using FastAPI and Docker.
- Simulate real-time review scoring using Kafka.
- Monitor model predictions, latency, logs, and drift using Prometheus and Grafana.
- Implement CI/CD for testing, validation, and deployment.
- Add explainability and responsible AI checks using SHAP, LIME, or feature importance.

## Proposed MLOps Architecture

```text
Review Data Sources
        |
        v
Batch Ingestion / Kafka Streaming
        |
        v
Data Cleaning and Validation
        |
        v
Feature Engineering
        |
        v
Model Training and Experiment Tracking
        |
        v
Model Registry and Versioning
        |
        v
FastAPI Model Serving
        |
        v
Real-Time Predictions and Campaign Detection
        |
        v
Monitoring, Logging, Drift Detection, and Alerts
        |
        v
CI/CD and Continuous Improvement
```

## Approach to Solve the Problem

### 1. Data Collection and Ingestion

The system will use review datasets containing review text, rating, timestamp, user ID, product ID, and related metadata. If real-time data is not directly available, a Kafka producer will simulate streaming reviews from a static dataset.

Data ingestion will include:

- Batch ingestion for historical training data.
- Streaming ingestion for real-time prediction.
- Raw data storage for traceability.
- Processed data storage for training and evaluation.

Recommended tools:

- Apache Airflow for scheduled batch pipelines.
- Kafka for real-time streaming.
- DVC for dataset versioning.

### 2. Data Validation and Preprocessing

Data quality checks will be applied before training and inference.

Checks include:

- Missing value detection.
- Duplicate review detection.
- Invalid rating detection.
- Empty or very short review detection.
- Abnormal timestamp detection.
- Language filtering.
- Class imbalance analysis.

Preprocessing includes:

- Text normalization.
- Stopword handling.
- Tokenization.
- Timestamp feature extraction.
- User and product activity aggregation.

### 3. Feature Engineering

The project will use a hybrid feature set instead of relying only on review text.

Textual features:

- TF-IDF vectors.
- Sentiment score.
- Review length.
- Keyword and phrase repetition.
- BERT or Sentence Transformer embeddings.

Behavioral features:

- Number of reviews per user.
- Average user rating.
- Review frequency per user.
- Number of products reviewed by a user.
- Repeated rating patterns.

Temporal features:

- Reviews per hour or day.
- Sudden burst activity.
- Time gap between reviews.
- Product-level rating spikes.

Similarity and campaign features:

- Semantic similarity between reviews.
- Similar reviews posted by different users.
- Groups of users reviewing the same product in a short time.
- Clustering-based suspicious campaign detection.

### 4. Model Development

The project will compare multiple models to create a strong research baseline.

Baseline models:

- Logistic Regression.
- Random Forest.
- XGBoost or LightGBM.

Advanced models:

- BERT or DistilBERT for review text classification.
- Sentence Transformer embeddings with classifier.
- Isolation Forest for anomaly detection.
- DBSCAN or KMeans for suspicious campaign clustering.

Model evaluation will compare:

- Text-only model.
- Text plus behavioral model.
- Text plus behavioral plus temporal model.
- Full hybrid model with campaign detection.

### 5. Experiment Tracking and Versioning

MLflow will be used to track:

- Model parameters.
- Evaluation metrics.
- Training artifacts.
- Confusion matrices.
- Model files.
- Dataset version references.

DVC will be used to version:

- Raw datasets.
- Processed datasets.
- Feature files.
- Trained model artifacts.

Git will be used for code collaboration and branching.

### 6. Real-Time Serving

The selected model will be deployed using FastAPI.

Main API endpoints:

- `POST /predict_review` for individual review prediction.
- `POST /detect_campaign` for campaign-level detection.
- `GET /health` for service health check.
- `GET /metrics` for monitoring metrics.

Kafka will simulate real-time incoming reviews. A Kafka consumer will send reviews to the FastAPI model service, receive predictions, and store the results.

### 7. Monitoring and Drift Detection

The deployed system will be monitored continuously.

Monitoring metrics:

- Prediction count.
- Fake review rate.
- Suspicious campaign count.
- Model confidence distribution.
- API latency.
- API error rate.
- Throughput.

Drift checks:

- Review length drift.
- Rating distribution drift.
- Sentiment distribution drift.
- Prediction distribution drift.
- Feature distribution drift.

Recommended tools:

- Prometheus for metrics collection.
- Grafana for dashboards.
- Python logging for application logs.

### 8. CI/CD and Governance

CI/CD will automate project validation and deployment steps.

Pipeline stages:

- Code linting.
- Unit testing.
- Data validation testing.
- Model training smoke test.
- Model evaluation check.
- API test.
- Docker image build.
- Deployment test.

Governance and responsible AI:

- Explainability using SHAP, LIME, or feature importance.
- Model card documentation.
- Dataset documentation.
- Bias and false positive analysis.
- Security checks for API input validation.

## Evaluation Metrics

Machine learning metrics:

- Accuracy.
- Precision.
- Recall.
- F1-score.
- ROC-AUC.
- PR-AUC.
- Confusion matrix.

Campaign detection metrics:

- Cluster purity.
- Suspicious group detection rate.
- Burst detection success rate.
- Anomaly detection precision.

MLOps and system metrics:

- Inference latency.
- Throughput.
- API error rate.
- Model drift score.
- Data drift score.
- Deployment success rate.
- Reproducibility using Git, DVC, and MLflow.

## Suggested Project Structure

```text
Bot_Campaign_Project/
  README.md
  data/
    raw/
    processed/
    features/
  dags/
  streaming/
    producer/
    consumer/
  src/
    data/
    features/
    models/
    campaign_detection/
    monitoring/
  api/
  tests/
  configs/
  notebooks/
  reports/
  docker-compose.yml
  Dockerfile
  requirements.txt
```

## Three-Person Work Split

The project can be fairly divided into three major modules. Each person owns a complete part of the pipeline, but all three should collaborate during integration, testing, and final presentation.

### Person 1: Data Engineering, Streaming, and Versioning

Main responsibility:

Build the complete data pipeline for batch and real-time review ingestion.

Tasks:

- Design the dataset schema.
- Collect or prepare the review dataset.
- Build raw-to-processed data pipeline.
- Create data cleaning and validation scripts.
- Set up Apache Airflow DAGs for batch processing.
- Build Kafka producer to simulate real-time incoming reviews.
- Build initial Kafka consumer for streaming data processing.
- Handle missing values, duplicates, invalid ratings, and text cleaning.
- Analyze class imbalance.
- Set up DVC for data versioning.
- Maintain data documentation.

Tools:

- Python.
- Pandas.
- Apache Airflow.
- Kafka.
- DVC.
- Git.

Deliverables:

- Data pipeline scripts.
- Airflow DAGs.
- Kafka streaming simulator.
- Cleaned and versioned dataset.
- Data validation report.
- Dataset documentation.

### Person 2: Feature Engineering, Model Development, and Experiment Tracking

Main responsibility:

Develop the machine learning models for fake review detection and campaign detection.

Tasks:

- Build text-based features using TF-IDF and embeddings.
- Build behavioral features from user and product activity.
- Build temporal features for burst and frequency analysis.
- Train baseline ML models.
- Train advanced NLP models such as BERT or DistilBERT.
- Build anomaly detection models for suspicious user behavior.
- Build clustering logic for campaign detection.
- Compare text-only, behavior-only, and hybrid models.
- Track all experiments using MLflow.
- Select the best model for deployment.
- Add explainability using SHAP, LIME, or feature importance.

Tools:

- Scikit-learn.
- PyTorch or TensorFlow.
- Transformers.
- MLflow.
- XGBoost or LightGBM.
- SHAP or LIME.

Deliverables:

- Feature engineering scripts.
- Model training scripts.
- MLflow experiment runs.
- Best trained model.
- Model evaluation report.
- Explainability report.

### Person 3: Deployment, Monitoring, CI/CD, and Integration

Main responsibility:

Deploy the model as a real-time service and connect the full MLOps pipeline.

Tasks:

- Build FastAPI prediction service.
- Create API endpoints for review prediction and campaign detection.
- Load the selected model from MLflow or saved artifacts.
- Connect Kafka consumer with the prediction API.
- Store prediction outputs.
- Dockerize the application.
- Create  Compose setup for services.
- Add Prometheus metrics.
- Build Grafana dashboard.
- Add logging for prediction and system events.
- Set up CI/CD using GitHub Actions or GitLab CI.
- Add tests for API, model loading, and pipeline components.
- Prepare final integrated demo.

Tools:

- FastAPI.
- Docker.
- Docker Compose.
- Kafka.
- Prometheus.
- Grafana.
- GitHub Actions or GitLab CI.
- Pytest.

Deliverables:

- FastAPI model service.
- Dockerized deployment.
- Real-time prediction workflow.
- Monitoring dashboard.
- CI/CD pipeline.
- API and integration tests.
- Final demo setup.

## Shared Responsibilities

All three members should contribute to:

- GitHub collaboration.
- Code reviews.
- Documentation.
- Architecture diagram.
- Final report.
- Final presentation.
- End-to-end testing.
- Demo preparation.

## Final Demo Flow

1. Historical review data is processed through the batch pipeline.
2. Model training is triggered and tracked using MLflow.
3. The best model is versioned and prepared for deployment.
4. Kafka streams new incoming reviews in real time.
5. FastAPI receives reviews and returns fake or genuine predictions.
6. Campaign detection checks for suspicious group behavior.
7. Predictions and metrics are logged.
8. Prometheus and Grafana show system and model monitoring dashboards.
9. Drift detection identifies changes in data or prediction patterns.
10. CI/CD validates the project using automated tests.

## Expected Outcome

The final system will demonstrate a research-standard MLOps pipeline for real-time fake review and bot campaign detection. It will combine machine learning, streaming data engineering, deployment, monitoring, drift detection, and governance into one complete production-style project.

# Dataset
se Amazon Reviews 2023 as the main dataset, and add a smaller labeled fake-review dataset for supervised fake/real training.
Recommended Dataset Setup
Purpose	Dataset	Why
Main MLOps + real-time pipeline	Amazon Reviews 2023	Huge, recent, has review text, rating, user ID, product ID, helpful votes, timestamp, verified purchase, metadata, and user-item graph signals
Fake review text classification	Deceptive Opinion Spam Corpus or MAiDE-up	Has labeled truthful/fake or real/AI-generated fake reviews
Optional behavior-rich alternative	Yelp Open Dataset	Good for reviews, users, businesses, check-ins, timestamps, but not direct fake labels

My Strong Recommendation
Use this combination:
Amazon Reviews 2023
Use for real-time streaming simulation.
Use for user behavior, product behavior, temporal burst detection, graph features, campaign detection.
Best for MLOps pipeline because it is large and realistic.

MAiDE-up Dataset
Use for detecting AI-generated fake reviews.
Good because fake reviews today can be LLM-generated.
It has real and AI-generated fake hotel reviews across multiple languages.

Deceptive Opinion Spam Corpus
Use as a classic benchmark for fake review classification.
Smaller but widely used in research.

Why Not Only One Dataset?
Because most open datasets have a limitation:
Amazon Reviews 2023: excellent metadata, but no direct fake label.
Yelp Open Dataset: rich behavior data, but no direct fake label.
Deceptive Opinion Spam Corpus: labeled fake/real, but small and mostly text-only.
MAiDE-up: good for AI fake reviews, but not enough for campaign behavior.
So for a research-standard project, combine them:
Amazon Reviews 2023 for real-time campaign detection + MAiDE-up/Ott corpus for labeled fake-review classification.

Best Final Choice for Your README
You can write:
The project will use Amazon Reviews 2023 as the primary dataset for building behavioral, temporal, and graph-based campaign detection features. Since this dataset does not provide direct fake-review labels, labeled deception datasets such as the Deceptive Opinion Spam Corpus and MAiDE-up will be used for supervised fake-review classification and benchmarking. This combination allows the system to support both individual review classification and coordinated campaign detection.

Sources:
Amazon Reviews 2023: https://amazon-reviews-2023.github.io/
MAiDE-up paper/dataset description: https://arxiv.org/abs/2404.12938
Deceptive Opinion Spam Corpus paper: https://arxiv.org/abs/1107.4557
Yelp Open Dataset: https://www.yelp.com/dataset
