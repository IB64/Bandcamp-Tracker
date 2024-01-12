# API Pipeline Script

This folder should contain all code and resources required to run the pipeline that connects to the [API found here](https://bandcamp.com/developer).
The files in this folder are used to connect to the API, extract and clean the information, then load it into the database.

## Installation and Requirements

It is recommended before stating any installations that you make a new virtual environment. This can be done through the commands in order:

- `python3 -m venv venv`
- `source venv/bin/activate`

Install all requirements for this folder through the command:\
`pip3 install -r requirements.txt`.

Create a `.env` file with the command:\
`touch .env`

**Required env variables**: 
- `DB_IP` -> The host name or address of a database server.
- `DB_USER` -> Username to access your database.
- `DB_PASSWORD` -> Password to access your database.
- `DB_NAME` -> The name of the database.
- `DB_PORT` -> The port number used for the database.


## Files
The files here serve different purposes:

### Pipeline
- `extract.py` - Calls the Bandcamp API and then webscrapes to extract information.
- `transform.py` - Transforms and cleans the extracted data.
- `load.py` - Loads transformed the data into a database.
- `pipeline.py` - Threads the previous three scripts into one pipeline to run the whole process.

### Dockerfile
 - `Dockerfile` - File needed to construct the image that can run the pipeline in a container.

### Testing
- `test_extract.py` - Test the extract script
- `test_transform.py` - Test the transform script
