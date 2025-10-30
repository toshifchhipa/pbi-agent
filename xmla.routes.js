const express = require('express');
const router = express.Router();
const XMLAController = require('../controllers/xmla.controller');
const { authenticate } = require('../middleware/auth.middleware');
const { setTenantContext } = require('../middleware/tenant.middleware');
const Joi = require('joi');

// Apply authentication and tenant context to all routes
router.use(authenticate);
router.use(setTenantContext);

// Validation schemas
const queryValidationSchema = Joi.object({
  query: Joi.string().required().min(1).max(10000)
});

// XMLA Connection Routes

/**
 * @route POST /xmla/datasets/:datasetId/test-connection
 * @desc Test XMLA connection to a dataset
 * @access Private
 */
router.post('/datasets/:datasetId/test-connection', XMLAController.testConnection);

/**
 * @route POST /xmla/datasets/:datasetId/execute
 * @desc Execute DAX query via XMLA interface
 * @access Private
 */
router.post('/datasets/:datasetId/execute', 
  (req, res, next) => {
    const { error } = queryValidationSchema.validate(req.body);
    if (error) {
      return res.status(400).json({
        error: 'Validation Error',
        message: error.details[0].message
      });
    }
    next();
  },
  XMLAController.executeQuery
);

/**
 * @route GET /xmla/datasets/:datasetId/metadata
 * @desc Get dataset metadata (tables, columns, measures)
 * @access Private
 */
router.get('/datasets/:datasetId/metadata', XMLAController.getMetadata);

/**
 * @route GET /xmla/datasets/:datasetId/tables
 * @desc Get list of tables in dataset
 * @access Private
 */
router.get('/datasets/:datasetId/tables', XMLAController.getTables);

/**
 * @route GET /xmla/datasets/:datasetId/measures
 * @desc Get list of measures in dataset
 * @access Private
 */
router.get('/datasets/:datasetId/measures', XMLAController.getMeasures);

/**
 * @route POST /xmla/datasets/:datasetId/common-queries
 * @desc Execute common DAX queries for dataset exploration
 * @access Private
 */
router.post('/datasets/:datasetId/common-queries', XMLAController.executeCommonQueries);

/**
 * @route POST /xmla/validate-query
 * @desc Validate DAX query syntax
 * @access Private
 */
router.post('/validate-query',
  (req, res, next) => {
    const { error } = queryValidationSchema.validate(req.body);
    if (error) {
      return res.status(400).json({
        error: 'Validation Error',
        message: error.details[0].message
      });
    }
    next();
  },
  XMLAController.validateQuery
);

// ============================================
// Day 15 Enhanced Endpoints
// ============================================

/**
 * @route GET /xmla/pool/stats
 * @desc Get connection pool statistics
 * @access Private
 */
router.get('/pool/stats', XMLAController.getPoolStats);

/**
 * @route POST /xmla/pool/test
 * @desc Test connection pool health
 * @access Private
 */
router.post('/pool/test', XMLAController.testConnectionPool);

/**
 * @route GET /xmla/executor/stats
 * @desc Get query executor statistics
 * @access Private
 */
router.get('/executor/stats', XMLAController.getExecutorStats);

/**
 * @route POST /xmla/datasets/:datasetId/extract-metadata
 * @desc Extract complete semantic model metadata
 * @access Private
 */
router.post('/datasets/:datasetId/extract-metadata', XMLAController.extractSemanticMetadata);

/**
 * @route POST /xmla/datasets/:datasetId/semantic-context
 * @desc Generate semantic context for AI
 * @access Private
 */
router.post('/datasets/:datasetId/semantic-context', XMLAController.getSemanticContext);

/**
 * @route GET /xmla/datasets/:datasetId/cached-metadata
 * @desc Get cached metadata from database
 * @access Private
 */
router.get('/datasets/:datasetId/cached-metadata', XMLAController.getCachedMetadata);

module.exports = router;
