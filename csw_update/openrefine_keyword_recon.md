# Using OpenRefine for geometadata keyword reconciliation
OpenRefine is an amazing tool for data clean-up. Of particular interest to metadata-ists is its ability to leverage controlled vocabularies.
##Install OpenRefine 2.6

##Add RDF extension 0.9

###download
https://github.com/fadmaa/grefine-rdf-extension/releases

unzip, add to extensions folder, rename to `rdf-extension`

##Add GEMET recon service to OpenRefine
Go to:
http://www.eionet.europa.eu/gemet/gemet-definitions.rdf?langcode=en

Download gemet-definitions.rdf

Start OpenRefine
Click RDF -> Add reconciliation service -> Based on RDF file

    Name: Gemet - RDF
    Upload file: gemet-definitions.rdf
    File Format: RDF/XML
    Label properties: check skos:prefLabel

Should upload and not crap out on you

split keywords_theme column via:
- click caret next to keywords_theme -> edit cells -> Split multi-valued cells (with ### as seperator)

Reconcile -> Start reconciling
Select Gemet - RDF
Click Start Reconciling
(this will take awhile, the % complete indicator is accurate, and it works steadily)

Go through and select matches (fairly conservatively)

Edit column -> Add column based on this column
call it NEW_keywords_theme_gemet_name
cell.recon.match.name

Edit column -> Add column based on this column
call it NEW_keywords_theme_gemet_id
cell.recon.match.id


## geoparsing abstracts for placenames

Install Vagrant using the appropriate download from [here](https://www.vagrantup.com/downloads.html). **Don't** use your system's package manager.

Clone [CLIFF-up](https://github.com/c4fcm/CLIFF-up) repository and follow the excellent instructions to get CLIFF running.

Click the caret to the left of the column you want to geoparse, then **Edit Column** and then **Add column by fetching URLs...**

Name it <original column name>_geoparsed. I'll use `abstract` and `abstract_geoparsed` in these directions.

Lower the throttling from 5000 ms to something much lower, like 500ms. We're working locally, so no need to space requests out too much.

Set the value equal to:

```
"http://localhost:8999/cliff-2.3.0/parse/text?q=" + escape(value,"url")
```

and let it crank. 

When it's done:

Add a new column based on `abstract_geoparsed` and call it `NEW_keywords_place_geonames`

join(
    uniques(
        forEach(
            parseJson(row.cells.abstract_geoparsed.value).results.places.mentions, 
            mention, 
            mention.name+"|http://sws.geonames.org/"+mention.id+"/about.rdf"
        )
    ), "###"
)




