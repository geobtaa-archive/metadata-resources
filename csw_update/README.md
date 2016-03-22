# Notes - CSW

Although we can't filter by Group (institution) within CSW itself, with GeoNetwork we can define Virtual CSWs that filter to return only results from a group. That is we can create a virtual CSW to correspond to each institution.

# spreadsheet input structure
- The input file format **must be** `csv`. Excel formats (`xls` or `xlsx`) are not supported.
- There **must be** a column called "uuid". The values in it should correspond to the fileIdentifier for the records being updated. For example: `bdfc5cd2-732d-4559-a9c7-df38dd683aec`.
- Currently, the following fields are acceptable for updating via spreadsheet and **must be** formatted exactly as written below to work
  + NEW_title
  + NEW_link_download
  + NEW_link_service_wms
  + NEW_link_service_esri
  + NEW_link_information
- The following fields are in the works
  + NEW_contact_organization
  + NEW_contact_individual
  + NEW_contact_name (for the contact element directly appended to MD_Metadata)
  + NEW_contact_organization (for the contact element directly appended to MD_Metadata)
  + NEW_keywords_theme
  + NEW_keywords_place

# Bonus OpenRefine Notes!
The CSV output from GeoNetwork can be loaded into (OpenRefine)[http://openrefine.org/]. OpenRefine has some features  
