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
    <xsl:param name="topic_category"/>

    <xsl:template match="node()|@*">
        <xsl:copy>
            <xsl:apply-templates select="node()|@*"/>
        </xsl:copy>
        
    </xsl:template>
    
    <xsl:template match="//gmd:topicCategory">
        <xsl:if test="not(./gmd:MD_TopicCategoryCode)">
            <gmd:topicCategory>
                <gmd:MD_TopicCategoryCode><xsl:value-of select="$topic_category"/></gmd:MD_TopicCategoryCode>    
            </gmd:topicCategory>
        </xsl:if>
        <xsl:if test="./gmd:MD_TopicCategoryCode and ./gmd:MD_TopicCategoryCode/text() !=''">
            <xsl:copy-of select="."/>
        </xsl:if>
    </xsl:template>
    
    <xsl:template name="tc">
            <xsl:copy-of select="."/>
            <gmd:topicCategory>
                <gmd:MD_TopicCategoryCode><xsl:value-of select="$topic_category"/></gmd:MD_TopicCategoryCode>    
            </gmd:topicCategory>
    </xsl:template>
    
    <xsl:template match="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent">
        <xsl:if test="not(../gmd:topicCategory)">
            <xsl:call-template name="tc"/>
        </xsl:if>
        <xsl:if test="../gmd:topicCategory">
            <xsl:copy-of select="."/>
        </xsl:if>
    </xsl:template>
</xsl:stylesheet>
