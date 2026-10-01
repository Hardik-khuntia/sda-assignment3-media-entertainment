# SDA Assignment 3 — Streaming Data Analytics Dashboard

## Overview

This project implements a streaming data analytics pipeline for a simulated Media & Entertainment platform. Kafka events are consumed, normalized and stored in MongoDB Atlas, where the processed data is used to build an analytical dashboard using MongoDB Atlas Charts.

## Architecture

Kafka Producers → Kafka Topics → `consumer.py` → MongoDB Atlas → MongoDB Atlas Charts

## Kafka Topics

The consumer reads streaming events from the following Kafka topics:

- `playback-events`
- `content-metadata`
- `subscription-events`
- `cdn-performance`
- `social-mentions`

## Consumer

The `consumer.py` application:

1. Connects to Kafka at `localhost:9092`
2. Consumes events from the five Kafka topics
3. Normalizes the incoming event structures
4. Adds source and Kafka topic information
5. Stores the processed documents in MongoDB Atlas

### MongoDB Storage

**Database:** `streaming_analytics`

**Collection:** `streaming_events`

MongoDB credentials and connection details are stored in environment variables and are not included in this repository.

## Dashboard

### Tool

**MongoDB Atlas Charts**

The dashboard contains 24 visualizations and KPI metrics covering:

- Content consumption
- Playback behaviour
- Viewer engagement
- Subscription behaviour
- CDN and streaming performance

### Key Visualizations

- Play Starts
- Average Buffering Rate
- Content Consumption by Title
- Playback Behaviour Mix
- Average CDN Latency by Region
- Average Buffering Rate by Region
- Subscription Lifecycle Mix
- Average Watch Completion by Title
- Streaming Activity by Hour
- Subscription Plan Mix
- Session Quality vs Watch Completion
- CDN Latency vs Average Buffering
- Content Engagement by Region
- Subscription Behaviour by Plan
- Content Volume vs Engagement
- Engagement by Device & Session Quality
- Playback Event Mix by Region
- Content Performance Matrix
- CDN Performance by Region & Node
- CDN Error Distribution
- Average Watch Completion
- Average CDN Latency
- CDN Error Events
- Total Streaming Events

## Setup

### 1. Install dependencies

```bash
py -m pip install -r requirements.txt