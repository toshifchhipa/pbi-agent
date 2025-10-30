const PowerBIAPIService = require('./powerbi-api.service');
const XMLAConnectionPoolService = require('./xmla-connection-pool.service');
const { logger } = require('../config/logger');

/**
 * XMLA Query Executor with Retry Logic
 * Handles DAX query execution with intelligent retry and error handling
 * Day 15: XMLA Connection Manager - Query Execution
 */
class XMLAQueryExecutorService {
  constructor() {
    this.powerbiAPI = PowerBIAPIService;
    this.connectionPool = XMLAConnectionPoolService;
    
    // Retry configuration
    this.retryConfig = {
      maxRetries: parseInt(process.env.XMLA_MAX_RETRIES || '3'),
      baseDelay: 1000, // 1 second
      maxDelay: 10000, // 10 seconds
      timeout: parseInt(process.env.XMLA_QUERY_TIMEOUT || '30000') // 30 seconds
    };

    // Query statistics
    this.stats = {
      totalQueries: 0,
      successfulQueries: 0,
      failedQueries: 0,
      retriedQueries: 0,
      totalExecutionTime: 0
    };
  }

  /**
   * Execute DAX query with retry logic
   */
  async executeDAXQuery(userId, tenantId, workspaceId, datasetId, daxQuery) {
    const startTime = Date.now();
    this.stats.totalQueries++;

    try {
      // Get connection from pool
      const connection = await this.connectionPool.getConnection(userId, tenantId, workspaceId);
      
      // Execute with retry
      const result = await this.executeWithRetry(
        userId,
        workspaceId,
        datasetId,
        daxQuery,
        connection
      );

      const executionTime = Date.now() - startTime;
      this.stats.successfulQueries++;
      this.stats.totalExecutionTime += executionTime;

      return {
        success: true,
        query: daxQuery,
        results: result,
        executionTimeMs: executionTime,
        metadata: {
          workspaceId,
          datasetId,
          rowCount: this.extractRowCount(result)
        }
      };
      
    } catch (error) {
      const executionTime = Date.now() - startTime;
      this.stats.failedQueries++;
      
      logger.error('DAX query execution failed:', {
        error: error.message,
        workspaceId,
        datasetId,
        executionTime
      });

      return {
        success: false,
        query: daxQuery,
        error: error.message,
        executionTimeMs: executionTime
      };
    }
  }

  /**
   * Execute with exponential backoff retry
   */
  async executeWithRetry(userId, workspaceId, datasetId, daxQuery, connection) {
    let lastError;
    let attempt = 0;

    while (attempt < this.retryConfig.maxRetries) {
      attempt++;
      
      try {
        logger.debug(`DAX query attempt ${attempt}/${this.retryConfig.maxRetries}`, {
          workspaceId,
          datasetId
        });

        // Execute the query
        const result = await this.executeSingleQuery(
          userId,
          workspaceId,
          datasetId,
          daxQuery
        );

        if (attempt > 1) {
          logger.info(`DAX query succeeded on attempt ${attempt}`);
          this.stats.retriedQueries++;
        }

        return result;
        
      } catch (error) {
        lastError = error;
        
        // Check if error is retryable
        const isRetryable = this.isRetryableError(error);
        
        if (!isRetryable) {
          logger.warn('Non-retryable error encountered', { error: error.message });
          break;
        }

        if (attempt >= this.retryConfig.maxRetries) {
          logger.error(`DAX query failed after ${attempt} attempts`);
          break;
        }

        // Calculate delay with exponential backoff
        const delay = this.calculateBackoffDelay(attempt);
        logger.warn(`Retrying DAX query in ${delay}ms... (attempt ${attempt + 1}/${this.retryConfig.maxRetries})`);
        
        await this.sleep(delay);

        // Check if we should invalidate the connection
        if (this.shouldInvalidateConnection(error)) {
          this.connectionPool.invalidateConnection(userId, tenantId, workspaceId);
          logger.info('Connection invalidated due to error');
        }
      }
    }

    throw lastError;
  }

  /**
   * Execute single DAX query
   */
  async executeSingleQuery(userId, workspaceId, datasetId, daxQuery) {
    try {
      // Create timeout promise
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => {
          reject(new Error(`Query timeout after ${this.retryConfig.timeout}ms`));
        }, this.retryConfig.timeout);
      });

      // Execute query with timeout
      const queryPromise = this.powerbiAPI.executeDAXQuery(
        userId,
        workspaceId,
        datasetId,
        daxQuery
      );

      const result = await Promise.race([queryPromise, timeoutPromise]);
      return result;
      
    } catch (error) {
      logger.error('Single query execution failed:', error);
      throw error;
    }
  }

  /**
   * Determine if error is retryable
   */
  isRetryableError(error) {
    // Network errors - retry
    if (!error.response) {
      return true;
    }

    const status = error.response?.status || error.status;

    // 5xx server errors - retry
    if (status >= 500) {
      return true;
    }

    // 429 rate limit - retry
    if (status === 429) {
      return true;
    }

    // 408 timeout - retry
    if (status === 408) {
      return true;
    }

    // 503 service unavailable - retry
    if (status === 503) {
      return true;
    }

    // Specific Power BI errors that are retryable
    const errorMessage = error.message?.toLowerCase() || '';
    if (errorMessage.includes('timeout') || 
        errorMessage.includes('temporarily unavailable') ||
        errorMessage.includes('capacity limit')) {
      return true;
    }

    // All other errors are not retryable
    return false;
  }

  /**
   * Check if connection should be invalidated
   */
  shouldInvalidateConnection(error) {
    const status = error.response?.status || error.status;
    
    // Invalidate on auth errors
    if (status === 401 || status === 403) {
      return true;
    }

    // Invalidate on specific Power BI errors
    const errorMessage = error.message?.toLowerCase() || '';
    if (errorMessage.includes('token') || 
        errorMessage.includes('authentication') ||
        errorMessage.includes('authorization')) {
      return true;
    }

    return false;
  }

  /**
   * Calculate exponential backoff delay
   */
  calculateBackoffDelay(attempt) {
    // Exponential backoff: 1s, 2s, 4s, 8s...
    const delay = Math.min(
      this.retryConfig.baseDelay * Math.pow(2, attempt - 1),
      this.retryConfig.maxDelay
    );

    // Add jitter (random 0-20% variation)
    const jitter = delay * 0.2 * Math.random();
    
    return Math.floor(delay + jitter);
  }

  /**
   * Sleep utility
   */
  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Extract row count from result
   */
  extractRowCount(result) {
    try {
      if (result && result.results && result.results.length > 0) {
        const firstResult = result.results[0];
        if (firstResult.tables && firstResult.tables.length > 0) {
          const rows = firstResult.tables[0].rows;
          return rows ? rows.length : 0;
        }
      }
      return 0;
    } catch (error) {
      logger.debug('Could not extract row count:', error.message);
      return 0;
    }
  }

  /**
   * Validate DAX query before execution
   */
  validateDAXQuery(daxQuery) {
    const errors = [];
    
    // Basic validation
    if (!daxQuery || typeof daxQuery !== 'string') {
      errors.push('Query must be a non-empty string');
      return { isValid: false, errors };
    }

    const trimmedQuery = daxQuery.trim();
    
    if (trimmedQuery.length === 0) {
      errors.push('Query cannot be empty');
      return { isValid: false, errors };
    }

    // Check for required DAX keywords
    const upperQuery = trimmedQuery.toUpperCase();
    
    if (!upperQuery.includes('EVALUATE') && !upperQuery.includes('DEFINE')) {
      errors.push('Query must contain EVALUATE or DEFINE statement');
    }

    // Check for balanced parentheses
    const openParens = (trimmedQuery.match(/\(/g) || []).length;
    const closeParens = (trimmedQuery.match(/\)/g) || []).length;
    
    if (openParens !== closeParens) {
      errors.push(`Unbalanced parentheses (${openParens} open, ${closeParens} close)`);
    }

    // Check for balanced square brackets
    const openBrackets = (trimmedQuery.match(/\[/g) || []).length;
    const closeBrackets = (trimmedQuery.match(/\]/g) || []).length;
    
    if (openBrackets !== closeBrackets) {
      errors.push(`Unbalanced square brackets (${openBrackets} open, ${closeBrackets} close)`);
    }

    // Check query length (prevent extremely large queries)
    if (trimmedQuery.length > 50000) {
      errors.push('Query exceeds maximum length of 50,000 characters');
    }

    // Warning for potentially dangerous operations (not an error, but logged)
    if (upperQuery.includes('DELETE') || upperQuery.includes('DROP')) {
      logger.warn('DAX query contains potentially dangerous operations', { query: daxQuery });
    }

    return {
      isValid: errors.length === 0,
      errors,
      warnings: []
    };
  }

  /**
   * Execute multiple DAX queries in parallel
   */
  async executeBatchQueries(userId, tenantId, workspaceId, datasetId, queries) {
    const startTime = Date.now();
    
    try {
      logger.info(`Executing batch of ${queries.length} DAX queries`);

      // Execute all queries in parallel
      const results = await Promise.all(
        queries.map(query => 
          this.executeDAXQuery(userId, tenantId, workspaceId, datasetId, query)
        )
      );

      const executionTime = Date.now() - startTime;

      return {
        success: true,
        totalQueries: queries.length,
        successfulQueries: results.filter(r => r.success).length,
        failedQueries: results.filter(r => !r.success).length,
        results,
        totalExecutionTimeMs: executionTime
      };
      
    } catch (error) {
      logger.error('Batch query execution failed:', error);
      throw error;
    }
  }

  /**
   * Get executor statistics
   */
  getStats() {
    const avgExecutionTime = this.stats.totalQueries > 0
      ? Math.round(this.stats.totalExecutionTime / this.stats.totalQueries)
      : 0;

    return {
      ...this.stats,
      averageExecutionTimeMs: avgExecutionTime,
      successRate: this.stats.totalQueries > 0
        ? ((this.stats.successfulQueries / this.stats.totalQueries) * 100).toFixed(2) + '%'
        : '0%'
    };
  }

  /**
   * Reset statistics
   */
  resetStats() {
    this.stats = {
      totalQueries: 0,
      successfulQueries: 0,
      failedQueries: 0,
      retriedQueries: 0,
      totalExecutionTime: 0
    };
    logger.info('Query executor statistics reset');
  }
}

module.exports = new XMLAQueryExecutorService();

