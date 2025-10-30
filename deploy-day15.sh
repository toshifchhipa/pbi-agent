#!/bin/bash

# ============================================
# Day 15: XMLA Connection Manager - Deployment Script
# ============================================
# Deploys all Day 15 files to remote server
# ============================================

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Configuration
REMOTE_USER="${REMOTE_USER:-ubuntu}"
REMOTE_HOST="${REMOTE_HOST:-ec2-13-127-85-241.ap-south-1.compute.amazonaws.com}"
REMOTE_DIR="/home/ubuntu/powerbi-saas-backend"

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║       DAY 15: XMLA CONNECTION MANAGER - DEPLOYMENT             ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Function to log steps
log_step() {
  echo -e "${BLUE}➜ $1${NC}"
}

log_success() {
  echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
  echo -e "${YELLOW}⚠️  $1${NC}"
}

# Check SSH connection
log_step "Testing SSH connection..."
if ssh -o ConnectTimeout=5 "$REMOTE_USER@$REMOTE_HOST" "echo 'Connected'" > /dev/null 2>&1; then
  log_success "SSH connection successful"
else
  echo "❌ Cannot connect to $REMOTE_HOST"
  echo "Please check:"
  echo "  1. Your SSH key is added: ssh-add ~/.ssh/your-key.pem"
  echo "  2. Remote host is correct: $REMOTE_HOST"
  echo "  3. Remote user is correct: $REMOTE_USER"
  exit 1
fi

# Create backup
log_step "Creating backup of existing files..."
ssh "$REMOTE_USER@$REMOTE_HOST" "
  cd $REMOTE_DIR
  BACKUP_DIR=\"backups/day15-backup-\$(date +%Y%m%d-%H%M%S)\"
  mkdir -p \$BACKUP_DIR/src/services
  mkdir -p \$BACKUP_DIR/src/controllers
  mkdir -p \$BACKUP_DIR/src/routes
  
  # Backup existing files if they exist
  [ -f src/services/xmla-connection.service.js ] && cp src/services/xmla-connection.service.js \$BACKUP_DIR/src/services/
  [ -f src/services/robust-xmla-connection.service.js ] && cp src/services/robust-xmla-connection.service.js \$BACKUP_DIR/src/services/
  [ -f src/controllers/xmla.controller.js ] && cp src/controllers/xmla.controller.js \$BACKUP_DIR/src/controllers/
  [ -f src/routes/xmla.routes.js ] && cp src/routes/xmla.routes.js \$BACKUP_DIR/src/routes/
  
  echo \"Backup created in \$BACKUP_DIR\"
"
log_success "Backup created"

# Deploy new service files
log_step "Deploying new Day 15 services..."
scp src/services/xmla-connection-pool.service.js \
    "$REMOTE_USER@$REMOTE_HOST:$REMOTE_DIR/src/services/"
log_success "Deployed xmla-connection-pool.service.js"

scp src/services/xmla-query-executor.service.js \
    "$REMOTE_USER@$REMOTE_HOST:$REMOTE_DIR/src/services/"
log_success "Deployed xmla-query-executor.service.js"

scp src/services/metadata-extractor.service.js \
    "$REMOTE_USER@$REMOTE_HOST:$REMOTE_DIR/src/services/"
log_success "Deployed metadata-extractor.service.js"

# Deploy updated controller
log_step "Deploying updated XMLA controller..."
scp src/controllers/xmla.controller.js \
    "$REMOTE_USER@$REMOTE_HOST:$REMOTE_DIR/src/controllers/"
log_success "Deployed xmla.controller.js"

# Deploy updated routes
log_step "Deploying updated XMLA routes..."
scp src/routes/xmla.routes.js \
    "$REMOTE_USER@$REMOTE_HOST:$REMOTE_DIR/src/routes/"
log_success "Deployed xmla.routes.js"

# Deploy test script
log_step "Deploying Day 15 test script..."
scp test-day15-xmla.sh \
    "$REMOTE_USER@$REMOTE_HOST:$REMOTE_DIR/"
ssh "$REMOTE_USER@$REMOTE_HOST" "chmod +x $REMOTE_DIR/test-day15-xmla.sh"
log_success "Deployed test-day15-xmla.sh"

# Deploy documentation
log_step "Deploying Day 15 documentation..."
scp DAY-15-XMLA-IMPLEMENTATION.md \
    "$REMOTE_USER@$REMOTE_HOST:$REMOTE_DIR/"
log_success "Deployed DAY-15-XMLA-IMPLEMENTATION.md"

# Check and update environment variables
log_step "Checking environment variables..."
ssh "$REMOTE_USER@$REMOTE_HOST" "
  cd $REMOTE_DIR
  
  # Check if Day 15 env vars exist
  if ! grep -q 'XMLA_CONNECTION_POOL_SIZE' .env 2>/dev/null; then
    echo '' >> .env
    echo '# Day 15: XMLA Configuration' >> .env
    echo 'XMLA_CONNECTION_POOL_SIZE=10' >> .env
    echo 'XMLA_QUERY_TIMEOUT=30000' >> .env
    echo 'XMLA_MAX_RETRIES=3' >> .env
    echo 'XMLA_ENABLE_LOGGING=true' >> .env
    echo 'Day 15 environment variables added'
  else
    echo 'Day 15 environment variables already exist'
  fi
"
log_success "Environment variables configured"

# Restart PM2
log_step "Restarting application..."
ssh "$REMOTE_USER@$REMOTE_HOST" "
  cd $REMOTE_DIR
  pm2 restart powerbi-backend
  sleep 3
  pm2 status
"
log_success "Application restarted"

# Verify deployment
log_step "Verifying deployment..."
sleep 2

ssh "$REMOTE_USER@$REMOTE_HOST" "
  cd $REMOTE_DIR
  
  echo 'Checking for new service files:'
  [ -f src/services/xmla-connection-pool.service.js ] && echo '  ✅ xmla-connection-pool.service.js' || echo '  ❌ xmla-connection-pool.service.js'
  [ -f src/services/xmla-query-executor.service.js ] && echo '  ✅ xmla-query-executor.service.js' || echo '  ❌ xmla-query-executor.service.js'
  [ -f src/services/metadata-extractor.service.js ] && echo '  ✅ metadata-extractor.service.js' || echo '  ❌ metadata-extractor.service.js'
  [ -f test-day15-xmla.sh ] && echo '  ✅ test-day15-xmla.sh' || echo '  ❌ test-day15-xmla.sh'
  [ -f DAY-15-XMLA-IMPLEMENTATION.md ] && echo '  ✅ DAY-15-XMLA-IMPLEMENTATION.md' || echo '  ❌ DAY-15-XMLA-IMPLEMENTATION.md'
  
  echo ''
  echo 'Checking PM2 process:'
  pm2 status | grep powerbi-backend
"
log_success "Deployment verified"

# Show PM2 logs
log_step "Showing recent logs..."
ssh "$REMOTE_USER@$REMOTE_HOST" "pm2 logs powerbi-backend --lines 20 --nostream"

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║              DAY 15 DEPLOYMENT COMPLETED! 🎉                   ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "📝 Next Steps:"
echo ""
echo "1. Test the deployment:"
echo "   ssh $REMOTE_USER@$REMOTE_HOST"
echo "   cd $REMOTE_DIR"
echo "   source .test-token"
echo "   ./test-day15-xmla.sh"
echo ""
echo "2. Check pool statistics:"
echo "   curl http://$REMOTE_HOST:3000/xmla/pool/stats -H \"Authorization: Bearer \$TOKEN\""
echo ""
echo "3. Monitor logs:"
echo "   pm2 logs powerbi-backend"
echo ""
echo "4. Check health:"
echo "   curl http://$REMOTE_HOST:3000/health"
echo ""
echo "📚 Documentation: $REMOTE_DIR/DAY-15-XMLA-IMPLEMENTATION.md"
echo ""

