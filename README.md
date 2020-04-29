# Stitch-API-python

Python wrapper for Stitch Data API

## Installation

```
pip install python-stitch-data==0.1.7
```

## Getting started

- Create secrets file
```
cp  .env.example .env
```

- Fill in secrets (or use environment variables)

### STITCH_API_KEY
stitch app > account settings.  Used for public API calls

### STITCH_CLIENT_ID
stitch app > account settings.  Used for public API calls

### STITCH_AUTH_USER
Used for internal api calls

### STITCH_AUTH_PASSWORD
Used for internal api calls

### STITCH_BLACKILST_SOURCES
To mitigate the risk of resetting a large table it can be desirable to add sources/streams to explicit blacklist 

## Usage

```
import stitch_api

stitch = stitch_api.StitchAPI(STITCH_API_KEY,
                              STITCH_CLIENT_ID,
                              STITCH_AUTH_USER,
                              STITCH_AUTH_PASSWORD,
                              STITCH_BLACKLIST_SOURCES)

sources = stitch.list_sources()
```


## CLI


```
stitchapi pause-source --source <SOURCE_NAME>
```