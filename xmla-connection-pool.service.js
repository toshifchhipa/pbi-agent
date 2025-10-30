const OAuthTokenModel = require('../models/oauth-token.model');
const { logger } = require('../config/logger');

/**
 * XMLA Connection Pool Manager
 * Manages connection pooling, token lifecycle, and connection reuse
 * Day 15: XMLA Connection Manager - Professional Implementation
 */
class XMLAConnectionPoolService {
  constructor() {
    // Connection pool storage
    this.connectionPool = new Map();
    
    // Pool configuration
    this.config = {
      maxPoolSize: parseInt(process.env.XMLA_CONNECTION_POOL_SIZE || '10'),
      maxConnectionAge: 30 * 60 * 1000, // 30 minutes
      tokenExpiryBuffer: 5 * 60 * 1000, // 5 minutes before expiry
      cleanupInterval: 5 * 60 * 1000 // Cleanup every 5 minutes
    };

    // Statistics tracking
    this.stats = {
      totalConnections: 0,
      activeConnections: 0,
      reuseCount: 0,
      expiryCount: 0,
      errorCount: 0
    };

    // Start periodic cleanup
    this.startCleanupJob();
    
    logger.info('XMLA Connection Pool initialized', this.config);
  }

  /**
   * Generate connection key for pooling
   */
  getConnectionKey(tenantId, workspaceId, userId) {
    return `${tenantId}:${workspaceId}:${userId}`;
  }

  /**
   * Get connection from pool or create new one
   */
  async getConnection(userId, tenantId, workspaceId) {
    const connectionKey = this.getConnectionKey(tenantId, workspaceId, userId);
    
    try {
      // Check if connection exists and is valid
      if (this.connectionPool.has(connectionKey)) {
        const connection = this.connectionPool.get(connectionKey);
        
        // Validate connection
        if (this.isConnectionValid(connection)) {
          logger.debug(`Reusing connection: ${connectionKey}`);
          this.stats.reuseCount++;
          connection.lastUsed = new Date();
          connection.usageCount++;
          return connection;
        } else {
          // Connection expired or invalid
          logger.debug(`Connection expired: ${connectionKey}`);
          this.connectionPool.delete(connectionKey);
          this.stats.expiryCount++;
        }
      }

      // Create new connection
      const connection = await this.createConnection(userId, tenantId, workspaceId);
      
      // Add to pool if space available
      if (this.connectionPool.size < this.config.maxPoolSize) {
        this.connectionPool.set(connectionKey, connection);
        logger.debug(`Added connection to pool: ${connectionKey} (Pool size: ${this.connectionPool.size})`);
      } else {
        // Pool is full, remove oldest connection
        const oldestKey = this.findOldestConnection();
        if (oldestKey) {
          this.connectionPool.delete(oldestKey);
          logger.debug(`Removed oldest connection: ${oldestKey}`);
        }
        this.connectionPool.set(connectionKey, connection);
      }

      this.stats.totalConnections++;
      this.stats.activeConnections = this.connectionPool.size;
      
      return connection;
      
    } catch (error) {
      this.stats.errorCount++;
      logger.error(`Failed to get connection: ${error.message}`);
      throw error;
    }
  }

  /**
   * Create new XMLA connection
   */
  async createConnection(userId, tenantId, workspaceId) {
    try {
      logger.info(`Creating new XMLA connection for workspace ${workspaceId}`);

      // Get fresh OAuth token
      const tokenData = await OAuthTokenModel.getValidToken(userId, 'powerbi');
      
      if (!tokenData) {
        throw new Error('No valid Power BI token found. Please reconnect your Power BI account.');
      }

      const connection = {
        // Identity
        workspaceId,
        tenantId,
        userId,
        
        // Token information
        accessToken: tokenData.access_token,
        tokenExpiry: new Date(tokenData.expires_at),
        
        // Connection metadata
        endpoint: this.buildXMLAEndpoint(workspaceId),
        connectionString: this.buildConnectionString(workspaceId, tokenData.access_token),
        
        // Lifecycle tracking
        createdAt: new Date(),
        lastUsed: new Date(),
        usageCount: 0,
        
        // Status
        isValid: true
      };

      logger.info(`XMLA connection created successfully for workspace ${workspaceId}`);
      return connection;
      
    } catch (error) {
      logger.error(`Failed to create XMLA connection: ${error.message}`);
      throw error;
    }
  }

  /**
   * Build XMLA endpoint URL
   */
  buildXMLAEndpoint(workspaceId) {
    return `powerbi://api.powerbi.com/v1.0/myorg/${workspaceId}`;
  }

  /**
   * Build connection string for XMLA
   */
  buildConnectionString(workspaceId, accessToken) {
    return {
      server: `powerbi://api.powerbi.com/v1.0/myorg/${workspaceId}`,
      database: workspaceId,
      authentication: 'Bearer Token',
      token: accessToken
    };
  }

  /**
   * Validate if connection is still usable
   */
  isConnectionValid(connection) {
    const now = new Date();
    
    // Check token expiry
    const timeUntilExpiry = connection.tokenExpiry.getTime() - now.getTime();
    if (timeUntilExpiry < this.config.tokenExpiryBuffer) {
      logger.debug('Connection token expired or expiring soon');
      return false;
    }

    // Check connection age
    const connectionAge = now.getTime() - connection.createdAt.getTime();
    if (connectionAge > this.config.maxConnectionAge) {
      logger.debug('Connection too old');
      return false;
    }

    // Check if marked as invalid
    if (!connection.isValid) {
      logger.debug('Connection marked as invalid');
      return false;
    }

    return true;
  }

  /**
   * Find oldest connection in pool
   */
  findOldestConnection() {
    let oldestKey = null;
    let oldestTime = Date.now();

    for (const [key, connection] of this.connectionPool.entries()) {
      const lastUsedTime = connection.lastUsed.getTime();
      if (lastUsedTime < oldestTime) {
        oldestTime = lastUsedTime;
        oldestKey = key;
      }
    }

    return oldestKey;
  }

  /**
   * Invalidate connection (mark for removal)
   */
  invalidateConnection(userId, tenantId, workspaceId) {
    const connectionKey = this.getConnectionKey(tenantId, workspaceId, userId);
    
    if (this.connectionPool.has(connectionKey)) {
      const connection = this.connectionPool.get(connectionKey);
      connection.isValid = false;
      logger.info(`Connection invalidated: ${connectionKey}`);
    }
  }

  /**
   * Remove connection from pool
   */
  removeConnection(userId, tenantId, workspaceId) {
    const connectionKey = this.getConnectionKey(tenantId, workspaceId, userId);
    
    if (this.connectionPool.has(connectionKey)) {
      this.connectionPool.delete(connectionKey);
      this.stats.activeConnections = this.connectionPool.size;
      logger.info(`Connection removed: ${connectionKey}`);
      return true;
    }
    
    return false;
  }

  /**
   * Periodic cleanup of expired connections
   */
  cleanupExpiredConnections() {
    const now = new Date();
    const keysToRemove = [];

    for (const [key, connection] of this.connectionPool.entries()) {
      if (!this.isConnectionValid(connection)) {
        keysToRemove.push(key);
      }
    }

    keysToRemove.forEach(key => {
      this.connectionPool.delete(key);
      this.stats.expiryCount++;
    });

    if (keysToRemove.length > 0) {
      logger.info(`Cleaned up ${keysToRemove.length} expired connections`);
      this.stats.activeConnections = this.connectionPool.size;
    }
  }

  /**
   * Start periodic cleanup job
   */
  startCleanupJob() {
    this.cleanupInterval = setInterval(() => {
      this.cleanupExpiredConnections();
    }, this.config.cleanupInterval);

    logger.info('XMLA connection pool cleanup job started');
  }

  /**
   * Stop cleanup job (for graceful shutdown)
   */
  stopCleanupJob() {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
      logger.info('XMLA connection pool cleanup job stopped');
    }
  }

  /**
   * Get pool statistics
   */
  getPoolStats() {
    return {
      ...this.stats,
      currentPoolSize: this.connectionPool.size,
      maxPoolSize: this.config.maxPoolSize,
      connections: Array.from(this.connectionPool.keys()).map(key => {
        const connection = this.connectionPool.get(key);
        return {
          key,
          createdAt: connection.createdAt,
          lastUsed: connection.lastUsed,
          usageCount: connection.usageCount,
          isValid: this.isConnectionValid(connection),
          tokenExpiresIn: Math.floor((connection.tokenExpiry.getTime() - Date.now()) / 1000) + 's'
        };
      })
    };
  }

  /**
   * Clear all connections (for testing or shutdown)
   */
  clearPool() {
    const size = this.connectionPool.size;
    this.connectionPool.clear();
    this.stats.activeConnections = 0;
    logger.info(`Cleared ${size} connections from pool`);
  }

  /**
   * Test connection health
   */
  async testConnection(userId, tenantId, workspaceId) {
    try {
      const connection = await this.getConnection(userId, tenantId, workspaceId);
      
      return {
        success: true,
        connectionKey: this.getConnectionKey(tenantId, workspaceId, userId),
        endpoint: connection.endpoint,
        tokenExpiresIn: Math.floor((connection.tokenExpiry.getTime() - Date.now()) / 1000),
        usageCount: connection.usageCount,
        createdAt: connection.createdAt,
        lastUsed: connection.lastUsed
      };
    } catch (error) {
      return {
        success: false,
        error: error.message
      };
    }
  }
}

module.exports = new XMLAConnectionPoolService();

