# Pipeline API Testing Guide

This guide provides instructions for testing the Pipeline Management API endpoints using curl commands. The API is running on port 8081.

> **Note:** The pipelines from the config file: `pipeline-config/pipeline-config.yaml` are started by default on app startup. So, when you start the app, check for running pipelines.

## Prerequisites

- The application is running on `localhost:8081`
- Configuration file is located at `pipeline-config/pipeline-config.yaml`
- curl is installed on your system
- Docker is installed (for Management Center)

## Hazelcast Management Center Setup

1. Start the Management Center:

```bash
docker run -d -p 8080:8080 hazelcast/management-center:latest
```

2. Access the Management Center:
   - Open <http://localhost:8080> in your browser
   - Create a new cluster connection with these details:
     - Cluster Name: dev
     - Member Address: 127.0.0.1:5701
     - Username: admin
     - Password: admin

3. After connecting, you can monitor:
   - Running pipelines
   - Cluster members
   - Pipeline metrics
   - Job status and statistics

## API Endpoints

### 1. Start All Pipelines

Starts all pipelines using the specified configuration file.

```bash
curl -X POST "http://localhost:8081/api/pipelines/start?configFilePath=pipeline-config/pipeline-config.yaml"
```

Expected Response:

- Success: HTTP 200 OK
- Error: HTTP 400 Bad Request (if config file path is invalid)
- Error: HTTP 500 Internal Server Error (if pipeline startup fails)

### 2. Stop a Specific Pipeline

Stops a pipeline by its job name.

```bash
curl -X POST "http://localhost:8081/api/pipelines/stop/{jobName}"
```

Replace `{jobName}` with the actual pipeline name.

Expected Response:

- Success: HTTP 200 OK
- Error: HTTP 404 Not Found (if pipeline doesn't exist)
- Error: HTTP 500 Internal Server Error

### 3. Stop All Pipelines

Stops all currently running pipelines.

```bash
curl -X POST "http://localhost:8081/api/pipelines/stop-all"
```

Expected Response:

- Success: HTTP 200 OK
- Error: HTTP 500 Internal Server Error

### 4. Get Pipeline Status

Retrieves the overall status of all pipelines.

```bash
curl "http://localhost:8081/api/pipelines/status"
```

Expected Response:

```json
{
    "runningCount": 2,
    "hasRunningPipelines": true
}
```

### 5. Get All Running Jobs

Retrieves detailed information about all running pipeline jobs.

```bash
curl "http://localhost:8081/api/pipelines/jobs"
```

Expected Response:

```json
{
    "jobName1": {
        "status": "RUNNING",
        "startTime": "2024-03-26T10:00:00Z"
    },
    "jobName2": {
        "status": "RUNNING",
        "startTime": "2024-03-26T10:05:00Z"
    }
}
```

### 6. Check Specific Pipeline Status

Checks if a specific pipeline is running.

```bash
curl "http://localhost:8081/api/pipelines/jobs/{jobName}/status"
```

Replace `{jobName}` with the actual pipeline name.

Expected Response:

- `true` if the pipeline is running
- `false` if the pipeline is not running

### 7. Get Running Pipeline Count

Returns the total number of currently running pipelines.

```bash
curl "http://localhost:8081/api/pipelines/count"
```

Expected Response:

```json
2
```

## Testing Workflow

Here's a recommended testing workflow:

1. Start all pipelines:

```bash
curl -X POST "http://localhost:8081/api/pipelines/start?configFilePath=pipeline-config/pipeline-config.yaml"
```

2. Check overall status:

```bash
curl "http://localhost:8081/api/pipelines/status"
```

3. Get detailed job information:

```bash
curl "http://localhost:8081/api/pipelines/jobs"
```

4. Check specific pipeline status:

```bash
curl "http://localhost:8081/api/pipelines/jobs/{jobName}/status"
```

5. Stop a specific pipeline:

```bash
curl -X POST "http://localhost:8081/api/pipelines/stop/{jobName}"
```

6. Stop all remaining pipelines:

```bash
curl -X POST "http://localhost:8081/api/pipelines/stop-all"
```

## Troubleshooting

1. If you get a 400 Bad Request when starting pipelines:
   - Verify the config file path is correct
   - Try using the absolute path to the config file
   - URL encode the path if it contains special characters

2. If you get a 404 Not Found when stopping a pipeline:
   - Verify the job name is correct
   - Check if the pipeline is actually running using the status endpoints

3. If you get a 500 Internal Server Error:
   - Check the application logs for detailed error messages
   - Verify the configuration file is valid
   - Ensure all required services are running

## Notes

- All timestamps are in UTC
- The API uses standard HTTP status codes
- All endpoints return JSON responses except for the status check endpoint which returns a boolean
- Make sure to replace `{jobName}` with actual pipeline names in the commands
