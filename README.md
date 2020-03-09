# Stitch-API-python

Python wrapper for Stitch Data API

## Installation

```
pip install python-stitch-data==0.1.1
```

## Getting started

- Create secrets file
```
cp  .env.example .env
```

- Fill in secrets (or use environment variables)

## Usage

```
import stitch_api

stitch = stitch_api.StitchAPI(STITCH_API_KEY,
                               STITCH_CLIENT_ID,
                               STITCH_AUTH_USER,
                               STITCH_AUTH_PASSWORD,
                               STITCH_BLACKILST_SOURCES)

sources = stitch.list_sources()
```


## CLI


```
stitchapi pause-source --source <SOURCE_NAME>
```