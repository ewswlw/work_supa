#!/usr/bin/env python3
"""
Multi-Tab dtale App for Bond G-Spread Analytics
==============================================

Enhanced dtale application that creates multiple views in a tabbed interface:
- All views available simultaneously
- Easy navigation between different data perspectives
- Performance optimized with smart sampling
- Team-friendly interface

Usage:
    poetry run python dtale_multi_tab_app.py
    poetry run python dtale_multi_tab_app.py --sample-size 25000
    poetry run python dtale_multi_tab_app.py --port 8050

Author: Trading Analytics Team
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np
import dtale
from pathlib import Path
from datetime import datetime
import logging
from typing import Dict, List, Optional, Tuple
import warnings
import time
import threading
from flask import Flask, render_template_string, request, jsonify
import webbrowser

warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import our existing dtale app
try:
    from .bond_analytics import BondAnalytics
except ImportError:
    # For direct execution, add parent directory to path
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from analytics.bond_analytics import BondAnalytics

class MultiTabDtaleApp:
    """
    Multi-tab dtale application for bond analytics.
    
    Creates multiple dtale instances for different views and presents them
    in a unified tabbed interface using Flask.
    """
    
    def __init__(self, data_path: str = "historical g spread/bond_z.parquet", 
                 sample_size: int = 25000, base_port: int = 40000, app_port: int = 8050):
        """
        Initialize the multi-tab dtale application.
        
        Args:
            data_path: Path to bond_z.parquet file
            sample_size: Number of rows to sample for performance
            base_port: Base port for dtale instances
            app_port: Port for the main Flask app
        """
        self.data_path = data_path
        self.sample_size = sample_size
        self.base_port = base_port
        self.app_port = app_port
        self.dtale_instances = {}
        self.flask_app = Flask(__name__)
        self.bond_app = BondAnalytics(data_path, sample_size, base_port)
        
        # Define which views to create tabs for
        self.tab_views = [
            'all',
            'cad-only', 
            'same-sector',
            'tradeable',
            'liquid',
            'cross-currency'
        ]
        
        self.setup_flask_routes()
    
    def setup_flask_routes(self):
        """Setup Flask routes for the tabbed interface."""
        
        @self.flask_app.route('/')
        def dashboard():
            """Main dashboard with tabbed interface."""
            return render_template_string(self.get_dashboard_template())
        
        @self.flask_app.route('/api/views')
        def get_views():
            """API endpoint to get available views."""
            views_info = []
            for view_name in self.tab_views:
                if view_name in self.dtale_instances:
                    instance = self.dtale_instances[view_name]
                    view_config = self.bond_app.view_definitions[view_name]
                    views_info.append({
                        'name': view_name,
                        'display_name': view_config['name'],
                        'description': view_config['description'],
                        'url': instance._url,
                        'active': True
                    })
                else:
                    view_config = self.bond_app.view_definitions[view_name]
                    views_info.append({
                        'name': view_name,
                        'display_name': view_config['name'],
                        'description': view_config['description'],
                        'url': None,
                        'active': False
                    })
            return jsonify(views_info)
        
        @self.flask_app.route('/api/stats')
        def get_stats():
            """API endpoint to get data statistics."""
            return jsonify(self.bond_app.stats)
    
    def get_dashboard_template(self):
        """Get the HTML template for the enhanced dashboard."""
        return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🚀 Bond G-Spread Analytics Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
        }
        
        .header {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            padding: 1rem 2rem;
            box-shadow: 0 2px 20px rgba(0,0,0,0.1);
            border-bottom: 1px solid rgba(255,255,255,0.2);
        }
        
        .header h1 {
            color: #2d3748;
            font-size: 1.8rem;
            font-weight: 700;
            margin-bottom: 0.5rem;
        }
        
        .stats-bar {
            display: flex;
            gap: 2rem;
            flex-wrap: wrap;
            align-items: center;
            color: #4a5568;
            font-size: 0.9rem;
        }
        
        .stat-item {
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        
        .stat-value {
            font-weight: 600;
            color: #2d3748;
        }

        /* Row Control Styling */
        .row-control {
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        .row-dropdown, .row-input {
            padding: 0.25rem 0.5rem;
            border: 1px solid #cbd5e0;
            border-radius: 4px;
            font-size: 0.85rem;
            background: white;
            color: #2d3748;
        }

        .row-dropdown:focus, .row-input:focus {
            outline: none;
            border-color: #667eea;
            box-shadow: 0 0 0 2px rgba(102, 126, 234, 0.2);
        }

        .row-input {
            width: 70px;
            text-align: center;
        }

        /* Enhanced Buttons */
        .btn {
            border: none;
            padding: 0.4rem 0.8rem;
            border-radius: 6px;
            cursor: pointer;
            font-weight: 500;
            font-size: 0.85rem;
            transition: all 0.3s ease;
            white-space: nowrap;
        }

        .btn-primary {
            background: #667eea;
            color: white;
        }

        .btn-primary:hover {
            background: #5a67d8;
        }

        .btn-secondary {
            background: #e2e8f0;
            color: #4a5568;
        }

        .btn-secondary:hover {
            background: #cbd5e0;
        }
        
        .tabs-container {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            margin: 0;
        }
        
        .tabs {
            display: flex;
            border-bottom: 2px solid #e2e8f0;
            overflow-x: auto;
            padding: 0 2rem;
        }
        
        .tab {
            padding: 1rem 1.5rem;
            cursor: pointer;
            border: none;
            background: none;
            color: #4a5568;
            font-weight: 500;
            border-bottom: 3px solid transparent;
            transition: all 0.3s ease;
            white-space: nowrap;
            position: relative;
        }
        
        .tab:hover {
            color: #2d3748;
            background: rgba(102, 126, 234, 0.1);
        }
        
        .tab.active {
            color: #667eea;
            border-bottom-color: #667eea;
            background: rgba(102, 126, 234, 0.1);
        }
        
        .tab.loading::after {
            content: "⏳";
            margin-left: 0.5rem;
        }
        
        .content {
            height: calc(100vh - 140px);
            position: relative;
            padding: 1rem;
        }
        
        .tab-content {
            display: none;
            width: 100%;
            height: 100%;
            position: relative;
        }
        
        .tab-content.active {
            display: block;
        }

        /* Resizable Container Styling */
        .resizable-container {
            width: 1200px;
            height: 600px;
            position: relative;
            background: white;
            border-radius: 8px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.1);
            overflow: hidden;
            margin: 0 auto;
            min-width: 400px;
            min-height: 300px;
            max-width: 95vw;
            max-height: 85vh;
        }

        .resizable-container iframe {
            width: 100%;
            height: calc(100% - 30px);
            border: none;
            background: white;
        }

        .resize-info {
            height: 30px;
            background: #f7fafc;
            border-bottom: 1px solid #e2e8f0;
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0 1rem;
            font-size: 0.8rem;
            color: #4a5568;
        }

        /* Resize Handles */
        .resize-handle {
            position: absolute;
            background: transparent;
            z-index: 10;
        }

        .resize-handle:hover {
            background: rgba(102, 126, 234, 0.2);
        }

        .resize-n {
            top: -3px;
            left: 10px;
            right: 10px;
            height: 6px;
            cursor: n-resize;
        }

        .resize-s {
            bottom: -3px;
            left: 10px;
            right: 10px;
            height: 6px;
            cursor: s-resize;
        }

        .resize-e {
            top: 10px;
            bottom: 10px;
            right: -3px;
            width: 6px;
            cursor: e-resize;
        }

        .resize-w {
            top: 10px;
            bottom: 10px;
            left: -3px;
            width: 6px;
            cursor: w-resize;
        }

        .resize-ne {
            top: -3px;
            right: -3px;
            width: 12px;
            height: 12px;
            cursor: ne-resize;
        }

        .resize-nw {
            top: -3px;
            left: -3px;
            width: 12px;
            height: 12px;
            cursor: nw-resize;
        }

        .resize-se {
            bottom: -3px;
            right: -3px;
            width: 12px;
            height: 12px;
            cursor: se-resize;
        }

        .resize-sw {
            bottom: -3px;
            left: -3px;
            width: 12px;
            height: 12px;
            cursor: sw-resize;
        }

        /* Dimension Tooltip */
        .dimension-tooltip {
            position: fixed;
            background: rgba(0, 0, 0, 0.8);
            color: white;
            padding: 0.5rem;
            border-radius: 4px;
            font-size: 0.85rem;
            pointer-events: none;
            z-index: 1000;
            display: none;
        }
        
        .loading-message {
            display: flex;
            align-items: center;
            justify-content: center;
            height: 100%;
            background: white;
            color: #4a5568;
            font-size: 1.1rem;
            border-radius: 8px;
        }
        
        .error-message {
            display: flex;
            align-items: center;
            justify-content: center;
            height: 100%;
            background: #fed7d7;
            color: #c53030;
            font-size: 1.1rem;
            border-radius: 8px;
        }
        
        @media (max-width: 768px) {
            .header {
                padding: 1rem;
            }
            
            .stats-bar {
                gap: 1rem;
                font-size: 0.8rem;
            }
            
            .tabs {
                padding: 0 1rem;
            }
            
            .tab {
                padding: 0.75rem 1rem;
                font-size: 0.9rem;
            }

            .resizable-container {
                width: 95vw;
                height: 70vh;
            }

            .content {
                padding: 0.5rem;
            }
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🚀 Bond G-Spread Analytics Dashboard</h1>
        <div class="stats-bar">
            <div class="stat-item">
                📊 <span class="stat-value" id="total-rows">Loading...</span> Total Rows
            </div>
            <div class="stat-item">
                📈 <span class="stat-value" id="total-columns">-</span> Columns
            </div>
            <div class="stat-item">
                💾 <span class="stat-value" id="memory-usage">-</span> Memory
            </div>
            <div class="stat-item">
                🎯 <span class="stat-value" id="sample-size">-</span> Sample Size
            </div>
            <div class="stat-item row-control">
                📄 Show:
                <select id="row-dropdown" class="row-dropdown" onchange="handleRowCountChange()">
                    <option value="50">50</option>
                    <option value="100" selected>100</option>
                    <option value="200">200</option>
                    <option value="all">All</option>
                    <option value="custom">Custom...</option>
                </select>
                <input type="number" id="row-input" class="row-input" min="1" max="50000" style="display: none;" onchange="handleCustomRowCount()" placeholder="1000">
                rows
            </div>
            <button class="btn btn-secondary" onclick="resetSize()">🔧 Reset Size</button>
            <button class="btn btn-primary" onclick="refreshStats()">🔄 Refresh</button>
        </div>
    </div>
    
    <div class="tabs-container">
        <div class="tabs" id="tabs">
            <!-- Tabs will be populated by JavaScript -->
        </div>
    </div>
    
    <div class="content" id="content">
        <!-- Tab content will be populated by JavaScript -->
    </div>

    <div class="dimension-tooltip" id="dimension-tooltip"></div>

    <script>
        let views = [];
        let activeTab = 'all';
        let tabSettings = {}; // Stores per-tab settings
        let isResizing = false;
        let resizeStartPos = null;
        let resizeStartSize = null;
        
        // Initialize tab settings with defaults
        function initTabSettings(tabName) {
            if (!tabSettings[tabName]) {
                tabSettings[tabName] = {
                    rowCount: getStoredRowCount(tabName) || 100,
                    width: getStoredSize(tabName)?.width || 1200,
                    height: getStoredSize(tabName)?.height || 600
                };
            }
        }

        // Local storage helpers
        function getStoredRowCount(tabName) {
            try {
                return localStorage.getItem(`dtale_rows_${tabName}`);
            } catch (e) {
                return null;
            }
        }

        function setStoredRowCount(tabName, count) {
            try {
                localStorage.setItem(`dtale_rows_${tabName}`, count);
            } catch (e) {
                console.warn('Could not save row count to localStorage');
            }
        }

        function getStoredSize(tabName) {
            try {
                const stored = localStorage.getItem(`dtale_size_${tabName}`);
                return stored ? JSON.parse(stored) : null;
            } catch (e) {
                return null;
            }
        }

        function setStoredSize(tabName, width, height) {
            try {
                localStorage.setItem(`dtale_size_${tabName}`, JSON.stringify({width, height}));
            } catch (e) {
                console.warn('Could not save size to localStorage');
            }
        }

        // Row count control handlers
        function handleRowCountChange() {
            const dropdown = document.getElementById('row-dropdown');
            const input = document.getElementById('row-input');
            
            if (dropdown.value === 'custom') {
                input.style.display = 'inline-block';
                input.focus();
                input.value = tabSettings[activeTab]?.rowCount || 100;
            } else {
                input.style.display = 'none';
                const newCount = dropdown.value;
                updateRowCount(newCount);
            }
        }

        function handleCustomRowCount() {
            const input = document.getElementById('row-input');
            const value = parseInt(input.value);
            
            if (value >= 1 && value <= 50000) {
                updateRowCount(value);
            } else {
                alert('Please enter a value between 1 and 50,000');
                input.value = tabSettings[activeTab]?.rowCount || 100;
            }
        }

        function updateRowCount(count) {
            if (!activeTab) return;
            
            initTabSettings(activeTab);
            tabSettings[activeTab].rowCount = count;
            setStoredRowCount(activeTab, count);
            
            // Show warning for large counts
            if (count === 'all' || (typeof count === 'number' && count > 5000)) {
                if (!confirm(`Loading ${count === 'all' ? 'all' : count + ''} rows may take some time and use significant memory. Continue?`)) {
                    // Reset to previous value
                    const dropdown = document.getElementById('row-dropdown');
                    dropdown.value = tabSettings[activeTab].rowCount < 50 ? '50' : 
                                   tabSettings[activeTab].rowCount < 200 ? '100' : '200';
                    return;
                }
            }
            
            refreshActiveTab();
        }

        function refreshActiveTab() {
            if (!activeTab) return;
            
            const contentElement = document.getElementById(`content-${activeTab}`);
            const container = contentElement.querySelector('.resizable-container');
            
            if (container) {
                const iframe = container.querySelector('iframe');
                const view = views.find(v => v.name === activeTab);
                
                if (view && view.url) {
                    const rowCount = tabSettings[activeTab]?.rowCount || 100;
                    const newUrl = `${view.url}${view.url.includes('?') ? '&' : '?'}rows=${rowCount}`;
                    iframe.src = newUrl;
                    
                    // Update info bar
                    updateResizeInfo(container);
                }
            }
        }

        function resetSize() {
            if (!activeTab) return;
            
            initTabSettings(activeTab);
            tabSettings[activeTab].width = 1200;
            tabSettings[activeTab].height = 600;
            setStoredSize(activeTab, 1200, 600);
            
            const contentElement = document.getElementById(`content-${activeTab}`);
            const container = contentElement.querySelector('.resizable-container');
            
            if (container) {
                container.style.width = '1200px';
                container.style.height = '600px';
                updateResizeInfo(container);
            }
        }

        // Resize functionality
        function makeResizable(container, tabName) {
            // Add resize handles
            const handles = ['n', 's', 'e', 'w', 'ne', 'nw', 'se', 'sw'];
            handles.forEach(handle => {
                const handleElement = document.createElement('div');
                handleElement.className = `resize-handle resize-${handle}`;
                handleElement.addEventListener('mousedown', (e) => startResize(e, handle, container, tabName));
                container.appendChild(handleElement);
            });
        }

        function startResize(e, direction, container, tabName) {
            e.preventDefault();
            e.stopPropagation();
            
            isResizing = true;
            resizeStartPos = { x: e.clientX, y: e.clientY };
            resizeStartSize = {
                width: parseInt(getComputedStyle(container).width),
                height: parseInt(getComputedStyle(container).height)
            };
            
            const tooltip = document.getElementById('dimension-tooltip');
            tooltip.style.display = 'block';
            
            const onMouseMove = (e) => handleResize(e, direction, container, tabName, tooltip);
            const onMouseUp = () => stopResize(tooltip, onMouseMove);
            
            document.addEventListener('mousemove', onMouseMove);
            document.addEventListener('mouseup', onMouseUp);
            
            document.body.style.cursor = getComputedStyle(e.target).cursor;
            document.body.style.userSelect = 'none';
        }

        function handleResize(e, direction, container, tabName, tooltip) {
            if (!isResizing) return;
            
            const deltaX = e.clientX - resizeStartPos.x;
            const deltaY = e.clientY - resizeStartPos.y;
            
            let newWidth = resizeStartSize.width;
            let newHeight = resizeStartSize.height;
            
            // Calculate new dimensions based on direction
            if (direction.includes('e')) newWidth += deltaX;
            if (direction.includes('w')) newWidth -= deltaX;
            if (direction.includes('s')) newHeight += deltaY;
            if (direction.includes('n')) newHeight -= deltaY;
            
            // Apply constraints
            newWidth = Math.max(400, Math.min(newWidth, window.innerWidth * 0.95));
            newHeight = Math.max(300, Math.min(newHeight, window.innerHeight * 0.85));
            
            // Apply new size
            container.style.width = newWidth + 'px';
            container.style.height = newHeight + 'px';
            
            // Update tooltip
            tooltip.textContent = `${newWidth} × ${newHeight}`;
            tooltip.style.left = (e.clientX + 10) + 'px';
            tooltip.style.top = (e.clientY - 30) + 'px';
            
            // Update info bar
            updateResizeInfo(container);
        }

        function stopResize(tooltip, onMouseMove) {
            if (!isResizing) return;
            
            isResizing = false;
            tooltip.style.display = 'none';
            
            document.removeEventListener('mousemove', onMouseMove);
            document.body.style.cursor = '';
            document.body.style.userSelect = '';
            
            // Save new size
            if (activeTab) {
                const container = document.getElementById(`content-${activeTab}`).querySelector('.resizable-container');
                if (container) {
                    const width = parseInt(container.style.width);
                    const height = parseInt(container.style.height);
                    
                    initTabSettings(activeTab);
                    tabSettings[activeTab].width = width;
                    tabSettings[activeTab].height = height;
                    setStoredSize(activeTab, width, height);
                }
            }
        }

        function updateResizeInfo(container) {
            const infoElement = container.querySelector('.resize-info-text');
            if (infoElement) {
                const width = parseInt(container.style.width);
                const height = parseInt(container.style.height);
                const rowCount = tabSettings[activeTab]?.rowCount || 100;
                infoElement.textContent = `${width} × ${height} • Showing ${rowCount === 'all' ? 'all' : rowCount} rows`;
            }
        }

        async function loadViews() {
            try {
                const response = await fetch('/api/views');
                views = await response.json();
                renderTabs();
                renderContent();
                
                // Activate first available tab
                const firstActive = views.find(v => v.active);
                if (firstActive) {
                    switchTab(firstActive.name);
                }
            } catch (error) {
                console.error('Error loading views:', error);
            }
        }
        
        async function loadStats() {
            try {
                const response = await fetch('/api/stats');
                const stats = await response.json();
                
                document.getElementById('total-rows').textContent = stats.total_rows?.toLocaleString() || 'Loading...';
                document.getElementById('total-columns').textContent = stats.total_columns || '-';
                document.getElementById('memory-usage').textContent = stats.memory_usage_mb ? 
                    `${stats.memory_usage_mb.toFixed(1)} MB` : '-';
                document.getElementById('sample-size').textContent = '25,000';
            } catch (error) {
                console.error('Error loading stats:', error);
            }
        }
        
        function renderTabs() {
            const tabsContainer = document.getElementById('tabs');
            tabsContainer.innerHTML = '';
            
            views.forEach(view => {
                const tab = document.createElement('button');
                tab.className = `tab ${view.active ? '' : 'loading'}`;
                tab.textContent = view.display_name;
                tab.onclick = () => switchTab(view.name);
                tab.id = `tab-${view.name}`;
                tabsContainer.appendChild(tab);
            });
        }
        
        function renderContent() {
            const contentContainer = document.getElementById('content');
            contentContainer.innerHTML = '';
            
            views.forEach(view => {
                const tabContent = document.createElement('div');
                tabContent.className = 'tab-content';
                tabContent.id = `content-${view.name}`;
                
                if (view.active && view.url) {
                    // Initialize tab settings
                    initTabSettings(view.name);
                    
                    // Create resizable container
                    const container = document.createElement('div');
                    container.className = 'resizable-container';
                    container.style.width = tabSettings[view.name].width + 'px';
                    container.style.height = tabSettings[view.name].height + 'px';
                    
                    // Add info bar
                    const infoBar = document.createElement('div');
                    infoBar.className = 'resize-info';
                    infoBar.innerHTML = `
                        <span class="resize-info-text">${tabSettings[view.name].width} × ${tabSettings[view.name].height} • Showing ${tabSettings[view.name].rowCount === 'all' ? 'all' : tabSettings[view.name].rowCount} rows</span>
                        <span style="font-size: 0.75rem; color: #718096;">Drag edges to resize</span>
                    `;
                    container.appendChild(infoBar);
                    
                    // Add iframe
                    const iframe = document.createElement('iframe');
                    const rowCount = tabSettings[view.name].rowCount;
                    iframe.src = `${view.url}${view.url.includes('?') ? '&' : '?'}rows=${rowCount}`;
                    iframe.title = view.display_name;
                    container.appendChild(iframe);
                    
                    // Make container resizable
                    makeResizable(container, view.name);
                    
                    tabContent.appendChild(container);
                } else if (view.active) {
                    tabContent.innerHTML = '<div class="loading-message">⏳ Loading view...</div>';
                } else {
                    tabContent.innerHTML = '<div class="error-message">❌ View not available. Check console for details.</div>';
                }
                
                contentContainer.appendChild(tabContent);
            });
        }
        
        function switchTab(viewName) {
            // Update active tab
            document.querySelectorAll('.tab').forEach(tab => {
                tab.classList.remove('active');
            });
            document.getElementById(`tab-${viewName}`).classList.add('active');
            
            // Update active content
            document.querySelectorAll('.tab-content').forEach(content => {
                content.classList.remove('active');
            });
            document.getElementById(`content-${viewName}`).classList.add('active');
            
            activeTab = viewName;
            
            // Update row count dropdown to reflect current tab settings
            initTabSettings(viewName);
            const currentRowCount = tabSettings[viewName].rowCount;
            const dropdown = document.getElementById('row-dropdown');
            const input = document.getElementById('row-input');
            
            if (['50', '100', '200', 'all'].includes(currentRowCount.toString())) {
                dropdown.value = currentRowCount.toString();
                input.style.display = 'none';
            } else {
                dropdown.value = 'custom';
                input.style.display = 'inline-block';
                input.value = currentRowCount;
            }
        }
        
        function refreshStats() {
            loadStats();
            loadViews();
        }
        
        // Initialize the dashboard
        document.addEventListener('DOMContentLoaded', function() {
            loadViews();
            loadStats();
        });
    </script>
</body>
</html>
        """
    
    def load_data(self) -> bool:
        """Load data using the bond app."""
        logger.info("Loading bond data...")
        success = self.bond_app.load_data()
        if success:
            logger.info(f"✅ Data loaded successfully: {self.bond_app.stats}")
        return success
    
    def create_all_views(self) -> Dict[str, bool]:
        """Create all dtale views for the tabs."""
        logger.info("Creating dtale views for tabs...")
        results = {}
        port_offset = 0
        
        for view_name in self.tab_views:
            try:
                logger.info(f"Creating view: {view_name}")
                
                # Set unique port for each view
                self.bond_app.dtale_manager.port_counter = port_offset
                
                # Create the view
                instance = self.bond_app.create_view(view_name)
                
                if instance:
                    self.dtale_instances[view_name] = instance
                    results[view_name] = True
                    logger.info(f"✅ Created {view_name} at {instance._url}")
                else:
                    results[view_name] = False
                    logger.warning(f"❌ Failed to create view: {view_name}")
                
                port_offset += 1
                
                # Small delay to ensure ports don't conflict
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error creating view {view_name}: {e}")
                results[view_name] = False
        
        return results
    
    def start_flask_app(self):
        """Start the Flask application."""
        logger.info(f"Starting Flask app on port {self.app_port}")
        self.flask_app.run(
            host='0.0.0.0',
            port=self.app_port,
            debug=False,
            use_reloader=False
        )
    
    def launch_dashboard(self) -> bool:
        """Launch the complete multi-tab dashboard."""
        try:
            # Load data
            if not self.load_data():
                logger.error("Failed to load data")
                return False
            
            # Create all views
            results = self.create_all_views()
            
            # Check if at least one view was created successfully
            successful_views = [view for view, success in results.items() if success]
            if not successful_views:
                logger.error("No views were created successfully")
                return False
            
            logger.info(f"✅ Successfully created {len(successful_views)} views: {successful_views}")
            
            # Print dashboard info
            self._print_dashboard_info(results)
            
            # Start Flask app in a separate thread
            flask_thread = threading.Thread(target=self.start_flask_app, daemon=True)
            flask_thread.start()
            
            # Give Flask a moment to start
            time.sleep(2)
            
            # Open browser
            dashboard_url = f"http://localhost:{self.app_port}"
            logger.info(f"🌐 Opening dashboard in browser: {dashboard_url}")
            try:
                webbrowser.open(dashboard_url)
            except Exception as e:
                logger.warning(f"Could not open browser automatically: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error launching dashboard: {e}")
            return False
    
    def _print_dashboard_info(self, results: Dict[str, bool]):
        """Print dashboard information."""
        print("\n" + "="*80)
        print("🚀 MULTI-TAB BOND G-SPREAD ANALYTICS DASHBOARD")
        print("="*80)
        print(f"📊 Total Data: {self.bond_app.stats['total_rows']:,} rows × {self.bond_app.stats['total_columns']} columns")
        print(f"🎯 Sample Size: {self.sample_size:,} rows per view")
        print(f"💾 Memory Usage: {self.bond_app.stats['memory_usage_mb']:.1f} MB")
        print(f"🌐 Dashboard URL: http://localhost:{self.app_port}")
        
        print("\n📋 AVAILABLE TABS:")
        for view_name in self.tab_views:
            view_config = self.bond_app.view_definitions[view_name]
            if results.get(view_name, False):
                instance = self.dtale_instances[view_name]
                status = f"🟢 ACTIVE - {instance._url}"
            else:
                status = "❌ FAILED"
            print(f"   {status} {view_config['name']}")
            print(f"      └─ {view_config['description']}")
        
        print("\n🎯 FEATURES:")
        print("   • Switch between views using tabs")
        print("   • Each tab shows a different data perspective")
        print("   • Excel-like interface with sorting, filtering, and export")
        print("   • Real-time data exploration and analysis")
        print("   • Performance optimized with smart sampling")
        
        print("\n⚡ PERFORMANCE NOTES:")
        print(f"   • Each view samples {self.sample_size:,} rows for optimal performance")
        print("   • Includes extreme Z-scores and portfolio holdings in samples")
        print("   • Use dtale's built-in filters for focused analysis")
        
        print("="*80)
    
    def cleanup(self):
        """Clean up dtale instances."""
        logger.info("Cleaning up dtale instances...")
        for view_name, instance in self.dtale_instances.items():
            try:
                instance.kill()
                logger.info(f"Closed {view_name}")
            except Exception as e:
                logger.warning(f"Error closing {view_name}: {e}")

def main():
    """Main application entry point."""
    parser = argparse.ArgumentParser(
        description="Multi-Tab dtale App for Bond G-Spread Analytics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    poetry run python dtale_multi_tab_app.py                    # Launch with default settings
    poetry run python dtale_multi_tab_app.py --sample-size 50000 # Use larger sample
    poetry run python dtale_multi_tab_app.py --port 8080        # Use different port
    poetry run python dtale_multi_tab_app.py --base-port 41000  # Use different dtale ports
        """
    )
    
    parser.add_argument(
        '--data-path', 
        default='historical g spread/bond_z.parquet',
        help='Path to bond_z.parquet file'
    )
    parser.add_argument(
        '--sample-size', 
        type=int, 
        default=25000,
        help='Number of rows to sample per view (default: 25000)'
    )
    parser.add_argument(
        '--base-port', 
        type=int, 
        default=40000,
        help='Base port number for dtale instances (default: 40000)'
    )
    parser.add_argument(
        '--port', 
        type=int, 
        default=8050,
        help='Port number for the main dashboard (default: 8050)'
    )
    
    args = parser.parse_args()
    
    # Initialize the app
    app = MultiTabDtaleApp(
        data_path=args.data_path,
        sample_size=args.sample_size,
        base_port=args.base_port,
        app_port=args.port
    )
    
    # Launch dashboard
    if not app.launch_dashboard():
        logger.error("Failed to launch dashboard. Exiting.")
        sys.exit(1)
    
    # Keep the app running
    try:
        print(f"\n🚀 Multi-tab dashboard running at: http://localhost:{args.port}")
        print("Press Ctrl+C to stop all servers")
        
        # Keep the main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n👋 Shutting down multi-tab dtale app...")
        app.cleanup()
        print("✅ Goodbye!")

if __name__ == "__main__":
    main() 