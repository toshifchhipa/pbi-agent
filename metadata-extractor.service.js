const XMLAQueryExecutorService = require('./xmla-query-executor.service');
const PowerBIAPIService = require('./powerbi-api.service');
const DatasetModel = require('../models/dataset.model');
const { logger } = require('../config/logger');
const { query } = require('../config/database');

/**
 * Semantic Model Metadata Extractor
 * Extracts comprehensive metadata from Power BI datasets
 * Day 15: XMLA Connection Manager - Metadata Extraction
 */
class MetadataExtractorService {
  constructor() {
    this.queryExecutor = XMLAQueryExecutorService;
    this.powerbiAPI = PowerBIAPIService;
  }

  /**
   * Extract complete metadata from dataset
   */
  async extractCompleteMetadata(userId, tenantId, workspaceId, datasetId) {
    const startTime = Date.now();
    
    try {
      logger.info(`Starting complete metadata extraction for dataset ${datasetId}`);

      // Get dataset from database
      const dataset = await DatasetModel.findByPowerBIId(workspaceId, datasetId, tenantId);
      if (!dataset) {
        throw new Error('Dataset not found in database');
      }

      // Extract using multiple methods in parallel
      const [tables, measures, relationships, columns] = await Promise.allSettled([
        this.extractTables(userId, tenantId, workspaceId, datasetId),
        this.extractMeasures(userId, tenantId, workspaceId, datasetId),
        this.extractRelationships(userId, tenantId, workspaceId, datasetId),
        this.extractColumns(userId, tenantId, workspaceId, datasetId)
      ]);

      const metadata = {
        datasetId: dataset.powerbi_dataset_id,
        datasetName: dataset.dataset_name,
        workspaceId: dataset.powerbi_workspace_id,
        tables: tables.status === 'fulfilled' ? tables.value : [],
        measures: measures.status === 'fulfilled' ? measures.value : [],
        relationships: relationships.status === 'fulfilled' ? relationships.value : [],
        columns: columns.status === 'fulfilled' ? columns.value : [],
        extractedAt: new Date().toISOString(),
        extractionTimeMs: Date.now() - startTime,
        extractionMethods: {
          tables: tables.status === 'fulfilled',
          measures: measures.status === 'fulfilled',
          relationships: relationships.status === 'fulfilled',
          columns: columns.status === 'fulfilled'
        }
      };

      // Store metadata in database
      await this.storeMetadata(dataset.dataset_id, metadata);

      logger.info(`Metadata extraction complete`, {
        datasetId,
        tablesCount: metadata.tables.length,
        measuresCount: metadata.measures.length,
        relationshipsCount: metadata.relationships.length,
        columnsCount: metadata.columns.length,
        timeMs: metadata.extractionTimeMs
      });

      return metadata;
      
    } catch (error) {
      logger.error(`Metadata extraction failed: ${error.message}`);
      throw error;
    }
  }

  /**
   * Extract tables from dataset
   */
  async extractTables(userId, tenantId, workspaceId, datasetId) {
    try {
      logger.debug(`Extracting tables for dataset ${datasetId}`);

      // DAX query to get table information
      const daxQuery = `
        EVALUATE
        SELECTCOLUMNS(
          FILTER(
            INFORMATION_SCHEMA_TABLES(),
            [TABLE_TYPE] <> "SYSTEM"
          ),
          "TableName", [TABLE_NAME],
          "TableType", [TABLE_TYPE],
          "Description", [DESCRIPTION]
        )
      `;

      const result = await this.queryExecutor.executeDAXQuery(
        userId,
        tenantId,
        workspaceId,
        datasetId,
        daxQuery
      );

      if (result.success && result.results?.results?.[0]?.tables?.[0]?.rows) {
        const rows = result.results.results[0].tables[0].rows;
        return rows.map(row => ({
          name: row['TableName'] || row[Object.keys(row)[0]],
          type: row['TableType'] || 'TABLE',
          description: row['Description'] || '',
          columnCount: 0 // Will be populated separately
        }));
      }

      // Fallback: Try REST API
      logger.debug('DAX query failed, trying REST API for tables');
      return await this.extractTablesViaREST(userId, workspaceId, datasetId);
      
    } catch (error) {
      logger.warn(`Table extraction failed: ${error.message}`);
      return [];
    }
  }

  /**
   * Extract tables using Power BI REST API (fallback)
   */
  async extractTablesViaREST(userId, workspaceId, datasetId) {
    try {
      const response = await this.powerbiAPI.makeRequest(
        userId,
        'GET',
        `/groups/${workspaceId}/datasets/${datasetId}/tables`
      );

      if (response?.value) {
        return response.value.map(table => ({
          name: table.name,
          type: 'TABLE',
          description: table.description || '',
          columnCount: table.columns?.length || 0,
          columns: table.columns?.map(col => ({
            name: col.name,
            dataType: col.dataType,
            isHidden: col.isHidden || false
          })) || []
        }));
      }

      return [];
    } catch (error) {
      logger.debug(`REST API table extraction failed: ${error.message}`);
      return [];
    }
  }

  /**
   * Extract columns for all tables
   */
  async extractColumns(userId, tenantId, workspaceId, datasetId) {
    try {
      logger.debug(`Extracting columns for dataset ${datasetId}`);

      const daxQuery = `
        EVALUATE
        SELECTCOLUMNS(
          INFORMATION_SCHEMA_COLUMNS(),
          "TableName", [TABLE_NAME],
          "ColumnName", [COLUMN_NAME],
          "DataType", [DATA_TYPE],
          "Description", [DESCRIPTION],
          "IsHidden", [IS_HIDDEN]
        )
      `;

      const result = await this.queryExecutor.executeDAXQuery(
        userId,
        tenantId,
        workspaceId,
        datasetId,
        daxQuery
      );

      if (result.success && result.results?.results?.[0]?.tables?.[0]?.rows) {
        const rows = result.results.results[0].tables[0].rows;
        return rows.map(row => ({
          tableName: row['TableName'] || row[Object.keys(row)[0]],
          name: row['ColumnName'] || row[Object.keys(row)[1]],
          dataType: row['DataType'] || 'Unknown',
          description: row['Description'] || '',
          isHidden: row['IsHidden'] || false
        }));
      }

      return [];
      
    } catch (error) {
      logger.warn(`Column extraction failed: ${error.message}`);
      return [];
    }
  }

  /**
   * Extract measures from dataset
   */
  async extractMeasures(userId, tenantId, workspaceId, datasetId) {
    try {
      logger.debug(`Extracting measures for dataset ${datasetId}`);

      const daxQuery = `
        EVALUATE
        SELECTCOLUMNS(
          INFORMATION_SCHEMA_MEASURES(),
          "MeasureName", [MEASURE_NAME],
          "TableName", [TABLE_NAME],
          "Expression", [EXPRESSION],
          "Description", [DESCRIPTION],
          "DataType", [DATA_TYPE]
        )
      `;

      const result = await this.queryExecutor.executeDAXQuery(
        userId,
        tenantId,
        workspaceId,
        datasetId,
        daxQuery
      );

      if (result.success && result.results?.results?.[0]?.tables?.[0]?.rows) {
        const rows = result.results.results[0].tables[0].rows;
        return rows.map(row => ({
          name: row['MeasureName'] || row[Object.keys(row)[0]],
          tableName: row['TableName'] || '',
          expression: row['Expression'] || '',
          description: row['Description'] || '',
          dataType: row['DataType'] || 'Variant'
        }));
      }

      // Fallback: Try REST API
      logger.debug('DAX query failed, trying REST API for measures');
      return await this.extractMeasuresViaREST(userId, workspaceId, datasetId);
      
    } catch (error) {
      logger.warn(`Measure extraction failed: ${error.message}`);
      return [];
    }
  }

  /**
   * Extract measures using Power BI REST API (fallback)
   */
  async extractMeasuresViaREST(userId, workspaceId, datasetId) {
    try {
      const response = await this.powerbiAPI.makeRequest(
        userId,
        'GET',
        `/groups/${workspaceId}/datasets/${datasetId}/measures`
      );

      if (response?.value) {
        return response.value.map(measure => ({
          name: measure.name,
          tableName: measure.tableName || '',
          expression: measure.expression || '',
          description: measure.description || '',
          dataType: 'Variant'
        }));
      }

      return [];
    } catch (error) {
      logger.debug(`REST API measure extraction failed: ${error.message}`);
      return [];
    }
  }

  /**
   * Extract relationships between tables
   */
  async extractRelationships(userId, tenantId, workspaceId, datasetId) {
    try {
      logger.debug(`Extracting relationships for dataset ${datasetId}`);

      const daxQuery = `
        EVALUATE
        SELECTCOLUMNS(
          INFORMATION_SCHEMA_RELATIONSHIPS(),
          "RelationshipName", [RELATIONSHIP_NAME],
          "FromTable", [FROM_TABLE],
          "FromColumn", [FROM_COLUMN],
          "ToTable", [TO_TABLE],
          "ToColumn", [TO_COLUMN],
          "CrossFilterDirection", [CROSS_FILTER_DIRECTION],
          "IsActive", [IS_ACTIVE]
        )
      `;

      const result = await this.queryExecutor.executeDAXQuery(
        userId,
        tenantId,
        workspaceId,
        datasetId,
        daxQuery
      );

      if (result.success && result.results?.results?.[0]?.tables?.[0]?.rows) {
        const rows = result.results.results[0].tables[0].rows;
        return rows.map(row => ({
          name: row['RelationshipName'] || '',
          fromTable: row['FromTable'] || row[Object.keys(row)[1]],
          fromColumn: row['FromColumn'] || row[Object.keys(row)[2]],
          toTable: row['ToTable'] || row[Object.keys(row)[3]],
          toColumn: row['ToColumn'] || row[Object.keys(row)[4]],
          crossFilterDirection: row['CrossFilterDirection'] || 'Single',
          isActive: row['IsActive'] !== false
        }));
      }

      return [];
      
    } catch (error) {
      logger.warn(`Relationship extraction failed: ${error.message}`);
      return [];
    }
  }

  /**
   * Extract hierarchies from dataset
   */
  async extractHierarchies(userId, tenantId, workspaceId, datasetId) {
    try {
      logger.debug(`Extracting hierarchies for dataset ${datasetId}`);

      const daxQuery = `
        EVALUATE
        SELECTCOLUMNS(
          INFORMATION_SCHEMA_HIERARCHIES(),
          "HierarchyName", [HIERARCHY_NAME],
          "TableName", [TABLE_NAME],
          "Description", [DESCRIPTION]
        )
      `;

      const result = await this.queryExecutor.executeDAXQuery(
        userId,
        tenantId,
        workspaceId,
        datasetId,
        daxQuery
      );

      if (result.success && result.results?.results?.[0]?.tables?.[0]?.rows) {
        const rows = result.results.results[0].tables[0].rows;
        return rows.map(row => ({
          name: row['HierarchyName'] || row[Object.keys(row)[0]],
          tableName: row['TableName'] || row[Object.keys(row)[1]],
          description: row['Description'] || ''
        }));
      }

      return [];
      
    } catch (error) {
      logger.warn(`Hierarchy extraction failed: ${error.message}`);
      return [];
    }
  }

  /**
   * Store metadata in database
   */
  async storeMetadata(datasetId, metadata) {
    try {
      await query(
        `UPDATE datasets 
         SET 
           tables = $1,
           measures = $2,
           relationships = $3,
           last_schema_sync = CURRENT_TIMESTAMP,
           schema_version = schema_version + 1,
           updated_at = CURRENT_TIMESTAMP
         WHERE dataset_id = $4`,
        [
          JSON.stringify(metadata.tables || []),
          JSON.stringify(metadata.measures || []),
          JSON.stringify(metadata.relationships || []),
          datasetId
        ]
      );

      logger.info(`Metadata stored in database for dataset ${datasetId}`);
    } catch (error) {
      logger.error(`Failed to store metadata: ${error.message}`);
      throw error;
    }
  }

  /**
   * Get cached metadata from database
   */
  async getCachedMetadata(datasetId, tenantId) {
    try {
      const result = await query(
        `SELECT 
          powerbi_dataset_id,
          dataset_name,
          powerbi_workspace_id,
          tables,
          measures,
          relationships,
          last_schema_sync,
          schema_version
         FROM datasets
         WHERE dataset_id = $1 AND tenant_id = $2 AND is_active = true`,
        [datasetId, tenantId]
      );

      if (result.rows.length === 0) {
        return null;
      }

      const row = result.rows[0];
      return {
        datasetId: row.powerbi_dataset_id,
        datasetName: row.dataset_name,
        workspaceId: row.powerbi_workspace_id,
        tables: row.tables || [],
        measures: row.measures || [],
        relationships: row.relationships || [],
        lastSync: row.last_schema_sync,
        schemaVersion: row.schema_version,
        isCached: true
      };
      
    } catch (error) {
      logger.error(`Failed to get cached metadata: ${error.message}`);
      return null;
    }
  }

  /**
   * Build business glossary from metadata
   */
  async buildBusinessGlossary(metadata) {
    const glossary = {
      tables: {},
      measures: {},
      relationships: []
    };

    // Organize tables
    metadata.tables.forEach(table => {
      glossary.tables[table.name] = {
        description: table.description || `${table.name} table`,
        type: table.type,
        columns: []
      };
    });

    // Add columns to tables
    metadata.columns?.forEach(column => {
      if (glossary.tables[column.tableName]) {
        glossary.tables[column.tableName].columns.push({
          name: column.name,
          dataType: column.dataType,
          description: column.description || `${column.name} column`,
          isHidden: column.isHidden
        });
      }
    });

    // Organize measures
    metadata.measures.forEach(measure => {
      glossary.measures[measure.name] = {
        description: measure.description || `${measure.name} measure`,
        expression: measure.expression,
        tableName: measure.tableName,
        dataType: measure.dataType
      };
    });

    // Add relationships
    glossary.relationships = metadata.relationships.map(rel => ({
      from: `${rel.fromTable}[${rel.fromColumn}]`,
      to: `${rel.toTable}[${rel.toColumn}]`,
      direction: rel.crossFilterDirection,
      isActive: rel.isActive
    }));

    return glossary;
  }

  /**
   * Generate semantic context for AI
   */
  async generateSemanticContext(userId, tenantId, workspaceId, datasetId) {
    try {
      // Get complete metadata
      const metadata = await this.extractCompleteMetadata(userId, tenantId, workspaceId, datasetId);
      
      // Build business glossary
      const glossary = await this.buildBusinessGlossary(metadata);

      // Generate human-readable context
      const context = {
        datasetName: metadata.datasetName,
        summary: {
          tableCount: metadata.tables.length,
          measureCount: metadata.measures.length,
          relationshipCount: metadata.relationships.length,
          columnCount: metadata.columns?.length || 0
        },
        tables: metadata.tables.map(table => ({
          name: table.name,
          description: table.description,
          columnCount: metadata.columns?.filter(c => c.tableName === table.name).length || 0
        })),
        measures: metadata.measures.map(measure => ({
          name: measure.name,
          description: measure.description,
          tableName: measure.tableName
        })),
        glossary,
        textContext: this.generateTextContext(metadata, glossary)
      };

      return context;
      
    } catch (error) {
      logger.error(`Failed to generate semantic context: ${error.message}`);
      throw error;
    }
  }

  /**
   * Generate text-based context for AI prompts
   */
  generateTextContext(metadata, glossary) {
    let text = `Dataset: ${metadata.datasetName}\n\n`;
    
    text += `Tables (${metadata.tables.length}):\n`;
    metadata.tables.forEach(table => {
      text += `- ${table.name}: ${table.description || 'No description'}\n`;
      const columns = metadata.columns?.filter(c => c.tableName === table.name) || [];
      if (columns.length > 0) {
        text += `  Columns: ${columns.map(c => c.name).join(', ')}\n`;
      }
    });

    text += `\nMeasures (${metadata.measures.length}):\n`;
    metadata.measures.forEach(measure => {
      text += `- ${measure.name}: ${measure.description || 'No description'}\n`;
    });

    text += `\nRelationships (${metadata.relationships.length}):\n`;
    metadata.relationships.forEach(rel => {
      text += `- ${rel.fromTable}[${rel.fromColumn}] → ${rel.toTable}[${rel.toColumn}]\n`;
    });

    return text;
  }
}

module.exports = new MetadataExtractorService();

