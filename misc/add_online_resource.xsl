<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:gco="http://www.isotc211.org/2005/gco"
    xmlns:gmd="http://www.isotc211.org/2005/gmd"
    xmlns:gmi="http://www.isotc211.org/2005/gmi"
    xmlns:gmx="http://www.isotc211.org/2005/gmx"
    xmlns:gsr="http://www.isotc211.org/2005/gsr"
    xmlns:gss="http://www.isotc211.org/2005/gss"
    xmlns:gts="http://www.isotc211.org/2005/gts"
    xmlns:gml="http://www.opengis.net/gml/3.2"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
   version="1.0" exclude-result-prefixes="gml gmd gco gmi xsl xs">
   <xsl:output version="1.0" omit-xml-declaration="yes" indent="no"/>
    <xsl:strip-space elements="*"/>
    <xsl:param name="url"/>
    <xsl:param name="protocol"/>
    <xsl:param name="description"/>
    
    <xsl:template match="node()|@*">
        <xsl:copy>
            <xsl:apply-templates select="node()|@*"/>
        </xsl:copy>
    </xsl:template>
    
    <xsl:template name="online_resource" match="gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions/gmd:MD_DigitalTransferOptions/gmd:onLine[last()]" >
        <xsl:copy-of select="."/>
        <gmd:onLine>
            <gmd:CI_OnlineResource>
                <gmd:linkage>
                    <gmd:URL><xsl:value-of select="$url"/></gmd:URL>
                </gmd:linkage>
                <gmd:protocol>
                    <gco:CharacterString><xsl:value-of select="$protocol"/></gco:CharacterString>
                </gmd:protocol>
                <gmd:description>
                    <gco:CharacterString><xsl:value-of select="$description"/></gco:CharacterString>
                </gmd:description>
            </gmd:CI_OnlineResource>
        </gmd:onLine>
    </xsl:template>
</xsl:stylesheet>