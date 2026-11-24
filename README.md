# Active Intelligence Visualization (AIV)

Welcome to **Active Intelligence Visualization (AIV)** — a powerful data visualization and reporting platform designed for enterprise environments. This guide provides everything you need to get AIV up and running in your environment, whether on-premise or in the cloud.

---

## Table of Contents

- [System Requirements](#system-requirements)
  - [Hardware Requirements](#hardware-requirements)
  - [Software Requirements](#software-requirements)
- [Installation & Deployment](#installation--deployment)
- [AI Module Installation](#ai-module-installation)
- [API Reference](#api-reference)
  - [File Upload API](#file-upload-api)
  - [Report Execution & Scheduling API](#report-execution--scheduling-api)
  - [Report Execution Status API](#report-execution-status-api)
  - [Report Sync Download API](#report-sync-download-api)
  - [Report Execution History API](#report-execution-history-api)
- [Support](#support)

---

## System Requirements

The following are the recommended hardware and software requirements for installing and running AIV. These requirements apply to both **on-premise** and **cloud-based** deployments.

### Hardware Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| **Processor** | 4-core CPU | 8 cores or more for better performance |
| **RAM** | 16 GB | 32 GB or more for optimal performance |
| **Disk Space** | 40 GB free | Additional storage for repositories, logs, and application data |
| **Memory Availability** | — | At least **80%** of system memory should be available to ensure stable operation |

### Software Requirements

| Component | Requirement |
|-----------|-------------|
| **Operating System** | Windows, Linux (including Red Hat Enterprise Linux), Unix, or macOS |
| **Java** | Java 17 or later |
| **Metadata Database** | PostgreSQL 17 |
| **Web Browser** | Latest version of Google Chrome, Mozilla Firefox, or Apple Safari |
| **Network** | Stable network connectivity with the required application ports configured |
| **Deployment Environment** | Supports both cloud-based and on-premise deployments |

---

## Installation & Deployment

AIV supports both **automated** and **manual** deployment methods. For detailed, step-by-step installation and upgrade instructions, please refer to the official deployment guide:

> 📘 **[AIV Deployment Guide](https://v7.docs.aivhub.com/aiv/installation_and_upgrade/aiv_deployment_guide)**

The deployment guide covers:

- Pre-installation setup and environment preparation
- Automated deployment steps
- Manual deployment steps
- Configuration and post-installation verification
- Upgrade procedures

---

## AI Module Installation

AIV includes an optional AI module that extends the platform with intelligent analytics capabilities. For installation instructions specific to the AI component, please refer to:

> 🤖 **[AIV AI Installation Guide](https://v7.docs.aivhub.com/aiv/installation_and_upgrade/aiv_ai_installation_guide/)**

---

## API Reference

AIV exposes a set of REST APIs that allow external systems to interact with the platform programmatically. Below is a summary of the available APIs.

### File Upload API

Upload files to AIV programmatically from external applications or automation scripts.

> 📄 **[File Upload API Documentation](https://v7.docs.aivhub.com/aiv/api/file_upload_api/)**

---

### Report Execution & Scheduling API

Schedule report executions and manage scheduling configurations via API.

> 📅 **[Report Execution & Scheduling API Documentation](https://v7.docs.aivhub.com/aiv/api/report_execution_scheduling/)**

---

### Report Execution Status API

Query the current execution status of reports to monitor progress and completion.

> 📊 **[Report Execution Status API Documentation](https://v7.docs.aivhub.com/aiv/api/report_execution_status/)**

---

### Report Sync Download API

Download executed reports synchronously for downstream processing or archival.

> 📥 **[Report Sync Download API Documentation](https://v7.docs.aivhub.com/aiv/api/report_sync_download/)**

---

### Report Execution History API

Retrieve a comprehensive list of all report executions, including their statuses. Supports filtering by date range for targeted lookups.

> 📋 **[Report Execution History API Documentation](https://v7.docs.aivhub.com/aiv/api/report_execution_history/)**

---

## Support

If you have any questions or need assistance with the installation or configuration of AIV, please do not hesitate to reach out to our support team. We are here to help ensure a smooth and successful deployment.

---

<p align="center">
  <em>Thank you for choosing Active Intelligence Visualization (AIV). We look forward to supporting your data-driven journey.</em>
</p>

