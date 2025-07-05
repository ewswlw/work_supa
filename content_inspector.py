#!/usr/bin/env python3
"""
Real-time Content Inspector for Bond Z-Score Dashboard
Captures and analyzes what's actually being rendered on the page
"""

import requests
import time
import json
import re
from datetime import datetime
from bs4 import BeautifulSoup
import subprocess

class ContentInspector:
    def __init__(self, base_url="http://localhost:8050"):
        self.base_url = base_url
        self.session = requests.Session()
        
    def capture_page_content(self):
        """Capture and analyze the current page content"""
        try:
            response = self.session.get(self.base_url, timeout=10)
            
            if response.status_code != 200:
                print(f"❌ HTTP Error: {response.status_code}")
                return None
            
            content = response.text
            
            # Parse HTML content
            soup = BeautifulSoup(content, 'html.parser')
            
            analysis = {
                'timestamp': datetime.now().isoformat(),
                'http_status': response.status_code,
                'content_length': len(content),
                'html_structure': self.analyze_html_structure(soup),
                'dash_components': self.find_dash_components(content),
                'javascript_errors': self.check_javascript_errors(content),
                'css_status': self.check_css_loading(soup),
                'data_elements': self.find_data_elements(soup),
                'interactive_elements': self.find_interactive_elements(soup)
            }
            
            return analysis
            
        except Exception as e:
            print(f"❌ Error capturing content: {e}")
            return None
    
    def analyze_html_structure(self, soup):
        """Analyze the HTML structure"""
        structure = {
            'title': soup.title.string if soup.title else "No title",
            'total_elements': len(soup.find_all()),
            'div_count': len(soup.find_all('div')),
            'script_count': len(soup.find_all('script')),
            'link_count': len(soup.find_all('link')),
            'meta_count': len(soup.find_all('meta')),
            'has_body': bool(soup.body),
            'has_head': bool(soup.head)
        }
        
        # Check for main app container
        app_container = soup.find('div', {'id': 'react-entry-point'}) or soup.find('div', class_=re.compile('dash'))
        structure['has_app_container'] = bool(app_container)
        
        return structure
    
    def find_dash_components(self, content):
        """Find Dash-specific components in the content"""
        dash_patterns = {
            'dash_core_components': r'dcc\.',
            'dash_html_components': r'html\.',
            'dash_bootstrap_components': r'dbc\.',
            'dash_table': r'dash_table|DataTable',
            'plotly_components': r'plotly|Plotly',
            'react_components': r'React\.|react',
            'dash_callbacks': r'@app\.callback|Output|Input|State'
        }
        
        components_found = {}
        for component_type, pattern in dash_patterns.items():
            matches = re.findall(pattern, content, re.IGNORECASE)
            components_found[component_type] = len(matches)
        
        return components_found
    
    def check_javascript_errors(self, content):
        """Check for potential JavaScript errors in the content"""
        error_patterns = [
            r'Uncaught.*Error',
            r'ReferenceError',
            r'TypeError',
            r'SyntaxError',
            r'console\.error',
            r'throw new Error'
        ]
        
        errors_found = []
        for pattern in error_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            if matches:
                errors_found.extend(matches)
        
        return errors_found
    
    def check_css_loading(self, soup):
        """Check CSS loading status"""
        css_links = soup.find_all('link', {'rel': 'stylesheet'})
        
        css_status = {
            'total_css_files': len(css_links),
            'bootstrap_loaded': False,
            'dash_css_loaded': False,
            'external_css_count': 0
        }
        
        for link in css_links:
            href = link.get('href', '')
            if 'bootstrap' in href.lower():
                css_status['bootstrap_loaded'] = True
            if 'dash' in href.lower():
                css_status['dash_css_loaded'] = True
            if href.startswith('http'):
                css_status['external_css_count'] += 1
        
        return css_status
    
    def find_data_elements(self, soup):
        """Find data-related elements"""
        data_elements = {
            'tables_found': len(soup.find_all('table')),
            'data_attributes': len(soup.find_all(attrs={'data-dash-is-loading': True})),
            'form_elements': len(soup.find_all(['input', 'select', 'textarea'])),
            'button_elements': len(soup.find_all('button')),
            'dropdown_elements': len(soup.find_all('select')),
            'filter_elements': 0
        }
        
        # Look for filter-related elements
        filter_keywords = ['filter', 'search', 'dropdown', 'range']
        for keyword in filter_keywords:
            elements = soup.find_all(attrs={'id': re.compile(keyword, re.IGNORECASE)})
            elements += soup.find_all(attrs={'class': re.compile(keyword, re.IGNORECASE)})
            data_elements['filter_elements'] += len(elements)
        
        return data_elements
    
    def find_interactive_elements(self, soup):
        """Find interactive elements"""
        interactive = {
            'clickable_elements': len(soup.find_all(['button', 'a'])),
            'input_elements': len(soup.find_all(['input', 'textarea'])),
            'select_elements': len(soup.find_all('select')),
            'dash_components': 0
        }
        
        # Look for Dash component IDs
        dash_ids = soup.find_all(attrs={'id': re.compile(r'.*-filter|.*-input|.*-button|.*-table')})
        interactive['dash_components'] = len(dash_ids)
        
        return interactive
    
    def print_content_report(self, analysis):
        """Print a comprehensive content report"""
        if not analysis:
            print("❌ No content analysis available")
            return
        
        print("🔍 DASHBOARD CONTENT INSPECTION REPORT")
        print("=" * 60)
        print(f"⏰ Timestamp: {analysis['timestamp']}")
        print(f"🌐 HTTP Status: {analysis['http_status']}")
        print(f"📏 Content Length: {analysis['content_length']:,} bytes")
        
        print("\n📱 HTML STRUCTURE:")
        structure = analysis['html_structure']
        print(f"  • Title: {structure['title']}")
        print(f"  • Total Elements: {structure['total_elements']}")
        print(f"  • DIV Elements: {structure['div_count']}")
        print(f"  • Script Tags: {structure['script_count']}")
        print(f"  • CSS Links: {structure['link_count']}")
        print(f"  • Has App Container: {'✅' if structure['has_app_container'] else '❌'}")
        
        print("\n⚛️  DASH COMPONENTS:")
        components = analysis['dash_components']
        for comp_type, count in components.items():
            status = "✅" if count > 0 else "❌"
            print(f"  • {comp_type.replace('_', ' ').title()}: {status} ({count} found)")
        
        print("\n🎨 CSS STATUS:")
        css = analysis['css_status']
        print(f"  • Total CSS Files: {css['total_css_files']}")
        print(f"  • Bootstrap Loaded: {'✅' if css['bootstrap_loaded'] else '❌'}")
        print(f"  • Dash CSS Loaded: {'✅' if css['dash_css_loaded'] else '❌'}")
        print(f"  • External CSS Files: {css['external_css_count']}")
        
        print("\n📊 DATA ELEMENTS:")
        data = analysis['data_elements']
        for element_type, count in data.items():
            print(f"  • {element_type.replace('_', ' ').title()}: {count}")
        
        print("\n🖱️  INTERACTIVE ELEMENTS:")
        interactive = analysis['interactive_elements']
        for element_type, count in interactive.items():
            print(f"  • {element_type.replace('_', ' ').title()}: {count}")
        
        if analysis['javascript_errors']:
            print("\n⚠️  JAVASCRIPT ERRORS DETECTED:")
            for error in analysis['javascript_errors']:
                print(f"  • {error}")
        else:
            print("\n✅ NO JAVASCRIPT ERRORS DETECTED")
    
    def save_content_snapshot(self, analysis):
        """Save content analysis to file"""
        if not analysis:
            return
        
        filename = f"content_snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(analysis, f, indent=2, default=str)
        
        print(f"📄 Content snapshot saved to {filename}")
    
    def continuous_inspection(self, interval=30):
        """Run continuous content inspection"""
        print(f"🔄 Starting continuous content inspection (every {interval}s)")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                print(f"\n{'='*60}")
                print(f"🔍 INSPECTION AT {datetime.now().strftime('%H:%M:%S')}")
                print('='*60)
                
                analysis = self.capture_page_content()
                if analysis:
                    self.print_content_report(analysis)
                    
                    # Check for issues
                    self.check_for_issues(analysis)
                
                print(f"\n⏰ Next inspection in {interval} seconds...")
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n📊 Content inspection stopped")
    
    def check_for_issues(self, analysis):
        """Check for potential issues in the content"""
        issues = []
        
        # Check for missing essential components
        if analysis['html_structure']['script_count'] == 0:
            issues.append("No JavaScript files detected")
        
        if not analysis['css_status']['bootstrap_loaded']:
            issues.append("Bootstrap CSS not detected")
        
        if analysis['data_elements']['tables_found'] == 0:
            issues.append("No data tables found")
        
        if analysis['interactive_elements']['dash_components'] == 0:
            issues.append("No Dash components detected")
        
        if analysis['javascript_errors']:
            issues.append(f"{len(analysis['javascript_errors'])} JavaScript errors detected")
        
        if issues:
            print("\n⚠️  POTENTIAL ISSUES DETECTED:")
            for issue in issues:
                print(f"  • {issue}")
        else:
            print("\n✅ NO ISSUES DETECTED - Dashboard appears healthy")

def main():
    """Main inspection function"""
    print("🚀 Bond Z-Score Dashboard Content Inspector")
    print("=" * 60)
    
    inspector = ContentInspector()
    
    if len(sys.argv) > 1 and sys.argv[1] == 'continuous':
        interval = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        inspector.continuous_inspection(interval)
    else:
        # Single inspection
        analysis = inspector.capture_page_content()
        if analysis:
            inspector.print_content_report(analysis)
            inspector.save_content_snapshot(analysis)
            inspector.check_for_issues(analysis)

if __name__ == '__main__':
    import sys
    main()