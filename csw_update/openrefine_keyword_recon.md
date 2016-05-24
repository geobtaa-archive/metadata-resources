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
