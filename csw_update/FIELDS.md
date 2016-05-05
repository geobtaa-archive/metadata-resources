### CSW Update Fields
When working with a CSV exported from GeoNetwork, you'll need to either:

1. **PREFERRED** Insert a new column, name it according to the table below and add your new values into it. (empty cells will be ignored, so if you want to null out a value, too bad I suppose. Let me know if you really need to do this and I can work in that functionality).
2. Change column names to the appropriate Column Header listed below (this is not really well tested, but ought to work, I guess)

| **Name**                       | **Column Header**        |
|:------------------------------:|:-------------------------|
| **Title**                      | NEW_title                |
| **Abstract**                   | NEW_abstract             |
| **Theme Keywords**             | NEW_keywords_theme       |
| **Place Keywords**             | NEW_keywords_place       |
| **ISO Topic Categories**       | NEW_topic_categories     |
| **Download Link**              | NEW_link_download        |
| **Information Link**           | NEW_link_information     |
| **Esri Web Service Link**      | NEW_link_service_esri    |
| **Web Map Service (WMS) Link** | NEW_link_service_wms     |
| **Distribution Format**        | NEW_distribution_format  |
| **Date Published**             | NEW_date_publication     |
| **Date Revised**               | NEW_date_revision        |
