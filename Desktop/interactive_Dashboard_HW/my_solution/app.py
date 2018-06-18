import os

import pandas as pd
import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

from flask import Flask, jsonify, render_template

# Create the Flask app
app = Flask(__name__)

# Create Database engine for given sql lite file
db_file = os.path.join('db', 'belly_button_biodiversity.sqlite')
engine = create_engine(f"sqlite:///{db_file}")

# reflect an existing database into a new model (copy the database into a new object)
Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Samples_Metadata = Base.classes.samples_metadata
OTU = Base.classes.otu
Samples = Base.classes.samples

# Create our session (link) from Python to the DB
session = Session(engine)

@app.route("/")
def index():
    # Return the Home Page
    return render_template("index.html")

@app.route("/names")
def names():
    # Use pandas to query statment column in the Samples table 
    stmt = session.query(Samples).statement
    # Save the statement column as a dataframe
    df = pd.read_sql_query(stmt, session.bind)
    # Set index of the dataframe to otu_id ??????
    df.set_index('otu_id', inplace=True)

    return jsonify(list(df.columns))



# List of OTU descriptions.
#Returns a list of OTU descriptions in the following format
#[
    # "Archaea;Euryarchaeota;Halobacteria;Halobacteriales;Halobacteriaceae;Halococcus",
    #"Archaea;Euryarchaeota;Halobacteria;Halobacteriales;Halobacteriaceae;Halococcus",
    #"Bacteria",
    #"Bacteria",
    #"Bacteria",
#   ...
#]
@app.route('/otu')
def otu():
    
    session_results = session.query(OTU.lowest_taxonomic_unit_found).all()
    
    # Use numpy ravel to extract list of tuples into a list of OTU descriptions
    otu_results = list(np.ravel(session_results))
    return jsonify(otu_results)


#"""MetaData for a given sample.
# Args: Sample in the format: `BB_940`
# Returns a json dictionary of sample metadata in the format
# {
    #AGE: 24,
    #BBTYPE: "I",
    #ETHNICITY: "Caucasian",
    #GENDER: "F",
    #LOCATION: "Beaufort/NC",
    #SAMPLEID: 940
# }
@app.route('/metadata/<sample>')
def sample_metadata(sample):
    # Return the MetaData for a given sample
    sel = [Samples_Metadata.SAMPLEID, Samples_Metadata.ETHNICITY,
           Samples_Metadata.GENDER, Samples_Metadata.AGE,
           Samples_Metadata.LOCATION, Samples_Metadata.BBTYPE]

    # sample[3:] strips the `BB_` prefix from the sample name to match
    # the numeric value of `SAMPLEID` from the database
    results = session.query(*sel).\
        filter(Samples_Metadata.SAMPLEID == sample[3:]).all()

    # Create a dictionary entry for each row of metadata information
    sample_metadata = {}
    for result in results:
        sample_metadata['SAMPLEID'] = result[0]
        sample_metadata['ETHNICITY'] = result[1]
        sample_metadata['GENDER'] = result[2]
        sample_metadata['AGE'] = result[3]
        sample_metadata['LOCATION'] = result[4]
        sample_metadata['BBTYPE'] = result[5]

    return jsonify(sample_metadata)

@app.route('/wfreq/<sample>')
def sample_wfreq(sample):
    # `sample[3:]` strips the `BB_` prefix
    results = session.query(Samples_Metadata.WFREQ).\
        filter(Samples_Metadata.SAMPLEID == sample[3:]).all()
    wfreq = np.ravel(results)
    """Return the Weekly Washing Frequency as a number."""

    """Args: Sample in the format: `BB_940`

    Returns an integer value for the weekly washing frequency `WFREQ`
    """
    # Return only the first integer value for washing frequency
    return jsonify(int(wfreq[0]))
    
@app.route('/samples/<sample>')
def samples(sample):
    stmt = session.query(Samples).statement
    df = pd.read_sql_query(stmt, session.bind)

    # Make sure that the sample was found in the columns, else throw an error
    if sample not in df.columns:
        return jsonify(f"Error! Sample: {sample} Not Found!"), 400

    # Return any sample values greater than 1
    df = df[df[sample] > 1]

    # Sort the results by sample in descending order
    df = df.sort_values(by=sample, ascending=0)

    # Format the data to send as json
    data = [{
        "otu_ids": df[sample].index.values.tolist(),
        "sample_values": df[sample].values.tolist()
    }]
    """Return a list dictionaries containing `otu_ids` and `sample_values`."""
    """OTU IDs and Sample Values for a given sample.

    Pandas DataFrame (OTU ID and Sample Value)
    in Descending Order by Sample Value

    Returns a list of dictionaries containing sorted lists for `otu_ids`
    and `sample_values`
    [
        {
            otu_ids: [
                1166,
                2858,
                481,
                ...
            ],
            sample_values: [
                163,
                126,
                113,
                ...
            ]
        }
    ]
    """
    return jsonify(data)

# allow user to run this app from terminal
if __name__ == "__main__":
    app.run(debug=True)








