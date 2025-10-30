const XMLAConnectionService = require('../services/xmla-connection.service');
const RobustXMLAConnectionService = require('../services/robust-xmla-connection.service');
const XMLAConnectionPoolService = require('../services/xmla-connection-pool.service');
const XMLAQueryExecutorService = require('../services/xmla-query-executor.service');
const MetadataExtractorService = require('../services/metadata-extractor.service');
const { logger } = require('../config/logger');

// Helper function to convert Power BI dataset ID to local dataset ID
async function getLocalDatasetId(datasetId, tenantId) {
  const isPowerBIDatasetId = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(datasetId);
  
  if (isPowerBIDatasetId) {
    const { query } = require('../config/database');
    const datasetResult = await query(
      'SELECT dataset_id FROM datasets WHERE powerbi_dataset_id = $1 AND tenant_id = $2 AND is_active = true',
      [datasetId, tenantId]
    );
    
    if (datasetResult.rows.length === 0) {
      throw new Error('Dataset not found. Please sync your workspace first.');
    }
    return datasetResult.rows[0].dataset_id;
  }
  
  return datasetId;
}

const XMLAController = {
  /**
   * Test XMLA connection to a dataset
   */
  async testConnection(req, res) {
    try {
      const { datasetId } = req.params;
      const userId = req.user.userId;
      const tenantId = req.user.tenantId;

      if (!datasetId) {
        return res.status(400).json({
          error: 'Bad Request',
          message: 'Dataset ID is required'
        });
      }

      const result = await XMLAConnectionService.testConnection(userId, tenantId, datasetId);

      if (result.success) {
        res.status(200).json({
          message: 'XMLA connection test successful',
          connectionString: result.connectionString,
          datasetName: result.datasetName,
          testResult: result.testResult
        });
      } else {
        res.status(400).json({
          error: 'Connection Failed',
          message: result.error
        });
      }
    } catch (error) {
      logger.error('XMLA connection test error:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: 'Failed to test XMLA connection'
      });
    }
  },

  /**
   * Execute DAX query via XMLA interface
   */
  async executeQuery(req, res) {
    try {
      const { datasetId } = req.params;
      const { query } = req.body;
      const userId = req.user.userId;
      const tenantId = req.user.tenantId;

      if (!datasetId) {
        return res.status(400).json({
          error: 'Bad Request',
          message: 'Dataset ID is required'
        });
      }

      if (!query) {
        return res.status(400).json({
          error: 'Bad Request',
          message: 'DAX query is required'
        });
      }

      // Validate query syntax
      const validation = XMLAConnectionService.validateQuery(query);
      if (!validation.isValid) {
        return res.status(400).json({
          error: 'Invalid Query',
          message: 'DAX query validation failed',
          errors: validation.errors
        });
      }

      const result = await XMLAConnectionService.executeQuery(userId, tenantId, datasetId, query);

      if (result.success) {
        res.status(200).json({
          message: 'Query executed successfully',
          query: result.query,
          results: result.results,
          executionTimeMs: result.executionTimeMs,
          datasetName: result.datasetName,
          rowCount: result.rowCount
        });
      } else {
        res.status(400).json({
          error: 'Query Execution Failed',
          message: result.error,
          query: result.query
        });
      }
    } catch (error) {
      logger.error('XMLA query execution error:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: 'Failed to execute DAX query'
      });
    }
  },

  /**
   * Get dataset metadata (tables, columns, measures)
   */
  async getMetadata(req, res) {
    try {
      const { datasetId } = req.params;
      const userId = req.user.userId;
      const tenantId = req.user.tenantId;

      if (!datasetId) {
        return res.status(400).json({
          error: 'Bad Request',
          message: 'Dataset ID is required'
        });
      }

      const localDatasetId = await getLocalDatasetId(datasetId, tenantId);
      const metadata = await RobustXMLAConnectionService.getDatasetMetadata(userId, tenantId, localDatasetId);

      res.status(200).json({
        message: 'Dataset metadata retrieved successfully',
        metadata
      });
    } catch (error) {
      logger.error('XMLA metadata retrieval error:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: 'Failed to retrieve dataset metadata'
      });
    }
  },

  /**
   * Get list of tables in dataset
   */
  async getTables(req, res) {
    try {
      const { datasetId } = req.params;
      const userId = req.user.userId;
      const tenantId = req.user.tenantId;

      if (!datasetId) {
        return res.status(400).json({
          error: 'Bad Request',
          message: 'Dataset ID is required'
        });
      }

      const localDatasetId = await getLocalDatasetId(datasetId, tenantId);
      const result = await RobustXMLAConnectionService.getTables(userId, tenantId, localDatasetId);

      if (result.success) {
        res.status(200).json({
          message: 'Tables retrieved successfully',
          tables: result.tables
        });
      } else {
        res.status(400).json({
          error: 'Failed to retrieve tables',
          message: result.error
        });
      }
    } catch (error) {
      logger.error('XMLA tables retrieval error:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: 'Failed to retrieve tables'
      });
    }
  },

  /**
   * Get list of measures in dataset
   */
  async getMeasures(req, res) {
    try {
      const { datasetId } = req.params;
      const userId = req.user.userId;
      const tenantId = req.user.tenantId;

      if (!datasetId) {
        return res.status(400).json({
          error: 'Bad Request',
          message: 'Dataset ID is required'
        });
      }

      const localDatasetId = await getLocalDatasetId(datasetId, tenantId);
      const result = await RobustXMLAConnectionService.getMeasures(userId, tenantId, localDatasetId);

      if (result.success) {
        res.status(200).json({
          message: 'Measures retrieved successfully',
          measures: result.measures
        });
      } else {
        res.status(400).json({
          error: 'Failed to retrieve measures',
          message: result.error
        });
      }
    } catch (error) {
      logger.error('XMLA measures retrieval error:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: 'Failed to retrieve measures'
      });
    }
  },

  /**
   * Execute common DAX queries for dataset exploration
   */
  async executeCommonQueries(req, res) {
    try {
      const { datasetId } = req.params;
      const userId = req.user.userId;
      const tenantId = req.user.tenantId;

      if (!datasetId) {
        return res.status(400).json({
          error: 'Bad Request',
          message: 'Dataset ID is required'
        });
      }

      const results = await XMLAConnectionService.executeCommonQueries(userId, tenantId, datasetId);

      res.status(200).json({
        message: 'Common queries executed successfully',
        results
      });
    } catch (error) {
      logger.error('XMLA common queries error:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: 'Failed to execute common queries'
      });
    }
  },

  /**
   * Validate DAX query syntax
   */
  async validateQuery(req, res) {
    try {
      const { query } = req.body;

      if (!query) {
        return res.status(400).json({
          error: 'Bad Request',
          message: 'DAX query is required'
        });
      }

      const validation = XMLAConnectionService.validateQuery(query);

      res.status(200).json({
        message: 'Query validation completed',
        isValid: validation.isValid,
        errors: validation.errors
      });
    } catch (error) {
      logger.error('XMLA query validation error:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: 'Failed to validate query'
      });
    }
  },

  /**
   * Get connection pool statistics (Day 15 Enhancement)
   * GET /xmla/pool/stats
   */
  async getPoolStats(req, res) {
    try {
      const stats = XMLAConnectionPoolService.getPoolStats();
      
      res.status(200).json({
        message: 'Connection pool statistics',
        stats
      });
    } catch (error) {
      logger.error('Failed to get pool stats:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: 'Failed to retrieve pool statistics'
      });
    }
  },

  /**
   * Test connection pool health (Day 15 Enhancement)
   * POST /xmla/pool/test
   */
  async testConnectionPool(req, res) {
    try {
      const { workspaceId } = req.body;
      const userId = req.user.userId;
      const tenantId = req.user.tenantId;

      if (!workspaceId) {
        return res.status(400).json({
          error: 'Bad Request',
          message: 'Workspace ID is required'
        });
      }

      const result = await XMLAConnectionPoolService.testConnection(userId, tenantId, workspaceId);

      res.status(result.success ? 200 : 500).json(result);
    } catch (error) {
      logger.error('Connection pool test failed:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: 'Failed to test connection pool'
      });
    }
  },

  /**
   * Get query executor statistics (Day 15 Enhancement)
   * GET /xmla/executor/stats
   */
  async getExecutorStats(req, res) {
    try {
      const stats = XMLAQueryExecutorService.getStats();
      
      res.status(200).json({
        message: 'Query executor statistics',
        stats
      });
    } catch (error) {
      logger.error('Failed to get executor stats:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: 'Failed to retrieve executor statistics'
      });
    }
  },

  /**
   * Extract complete semantic model metadata (Day 15 Enhancement)
   * POST /xmla/datasets/:datasetId/extract-metadata
   */
  async extractSemanticMetadata(req, res) {
    try {
      const { datasetId } = req.params;
      const userId = req.user.userId;
      const tenantId = req.user.tenantId;

      if (!datasetId) {
        return res.status(400).json({
          error: 'Bad Request',
          message: 'Dataset ID is required'
        });
      }

      // Get local dataset ID and workspace
      const localDatasetId = await getLocalDatasetId(datasetId, tenantId);
      
      // Get dataset details to find workspace
      const { query } = require('../config/database');
      const datasetResult = await query(
        'SELECT powerbi_workspace_id FROM datasets WHERE dataset_id = $1 AND tenant_id = $2',
        [localDatasetId, tenantId]
      );

      if (datasetResult.rows.length === 0) {
        return res.status(404).json({
          error: 'Not Found',
          message: 'Dataset not found'
        });
      }

      const workspaceId = datasetResult.rows[0].powerbi_workspace_id;

      // Extract metadata
      const metadata = await MetadataExtractorService.extractCompleteMetadata(
        userId,
        tenantId,
        workspaceId,
        datasetId
      );

      res.status(200).json({
        message: 'Semantic model metadata extracted successfully',
        metadata
      });
    } catch (error) {
      logger.error('Semantic metadata extraction error:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: 'Failed to extract semantic metadata'
      });
    }
  },

  /**
   * Generate semantic context for AI (Day 15 Enhancement)
   * POST /xmla/datasets/:datasetId/semantic-context
   */
  async getSemanticContext(req, res) {
    try {
      const { datasetId } = req.params;
      const userId = req.user.userId;
      const tenantId = req.user.tenantId;

      if (!datasetId) {
        return res.status(400).json({
          error: 'Bad Request',
          message: 'Dataset ID is required'
        });
      }

      // Get local dataset ID and workspace
      const localDatasetId = await getLocalDatasetId(datasetId, tenantId);
      
      const { query } = require('../config/database');
      const datasetResult = await query(
        'SELECT powerbi_workspace_id FROM datasets WHERE dataset_id = $1 AND tenant_id = $2',
        [localDatasetId, tenantId]
      );

      if (datasetResult.rows.length === 0) {
        return res.status(404).json({
          error: 'Not Found',
          message: 'Dataset not found'
        });
      }

      const workspaceId = datasetResult.rows[0].powerbi_workspace_id;

      // Generate semantic context
      const context = await MetadataExtractorService.generateSemanticContext(
        userId,
        tenantId,
        workspaceId,
        datasetId
      );

      res.status(200).json({
        message: 'Semantic context generated successfully',
        context
      });
    } catch (error) {
      logger.error('Semantic context generation error:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: 'Failed to generate semantic context'
      });
    }
  },

  /**
   * Get cached metadata from database (Day 15 Enhancement)
   * GET /xmla/datasets/:datasetId/cached-metadata
   */
  async getCachedMetadata(req, res) {
    try {
      const { datasetId } = req.params;
      const tenantId = req.user.tenantId;

      if (!datasetId) {
        return res.status(400).json({
          error: 'Bad Request',
          message: 'Dataset ID is required'
        });
      }

      const localDatasetId = await getLocalDatasetId(datasetId, tenantId);
      const metadata = await MetadataExtractorService.getCachedMetadata(localDatasetId, tenantId);

      if (!metadata) {
        return res.status(404).json({
          error: 'Not Found',
          message: 'No cached metadata found. Please extract metadata first.'
        });
      }

      res.status(200).json({
        message: 'Cached metadata retrieved successfully',
        metadata
      });
    } catch (error) {
      logger.error('Failed to get cached metadata:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: 'Failed to retrieve cached metadata'
      });
    }
  }
};

module.exports = XMLAController;
