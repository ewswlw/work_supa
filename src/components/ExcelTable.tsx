import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { 
  ChevronDown, 
  ChevronUp, 
  Filter, 
  Download, 
  Upload,
  Search,
  RefreshCw,
  Settings,
  Eye,
  EyeOff
} from 'lucide-react';

interface TableData {
  [key: string]: any;
}

interface SortConfig {
  key: string;
  direction: 'asc' | 'desc';
}

interface FilterConfig {
  [key: string]: {
    value: string;
    type: 'text' | 'number' | 'date' | 'select';
    operator: 'equals' | 'contains' | 'greater' | 'less' | 'between';
  };
}

interface ExcelTableProps {
  data: TableData[];
  onDataChange?: (data: TableData[]) => void;
}

const ExcelTable: React.FC<ExcelTableProps> = ({ data, onDataChange }) => {
  const [sortConfig, setSortConfig] = useState<SortConfig | null>(null);
  const [filters, setFilters] = useState<FilterConfig>({});
  const [selectedCells, setSelectedCells] = useState<Set<string>>(new Set());
  const [hiddenColumns, setHiddenColumns] = useState<Set<string>>(new Set());
  const [searchTerm, setSearchTerm] = useState('');
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [editingCell, setEditingCell] = useState<{row: number, col: string} | null>(null);
  const [editValue, setEditValue] = useState('');

  // Get all column names from data
  const columns = useMemo(() => {
    if (!data || data.length === 0) return [];
    return Object.keys(data[0]);
  }, [data]);

  // Get visible columns
  const visibleColumns = useMemo(() => {
    return columns.filter(col => !hiddenColumns.has(col));
  }, [columns, hiddenColumns]);

  // Filter and sort data
  const filteredAndSortedData = useMemo(() => {
    if (!data) return [];
    
    let filtered = [...data];
    
    // Apply search filter
    if (searchTerm) {
      filtered = filtered.filter(row =>
        Object.values(row).some(value =>
          String(value).toLowerCase().includes(searchTerm.toLowerCase())
        )
      );
    }
    
    // Apply column filters
    Object.entries(filters).forEach(([key, filterConfig]) => {
      filtered = filtered.filter(row => {
        const value = row[key];
        const filterValue = filterConfig.value;
        
        if (!filterValue) return true;
        
        switch (filterConfig.operator) {
          case 'contains':
            return String(value).toLowerCase().includes(filterValue.toLowerCase());
          case 'equals':
            return String(value) === filterValue;
          case 'greater':
            return Number(value) > Number(filterValue);
          case 'less':
            return Number(value) < Number(filterValue);
          default:
            return true;
        }
      });
    });
    
    // Apply sorting
    if (sortConfig) {
      filtered.sort((a, b) => {
        const aValue = a[sortConfig.key];
        const bValue = b[sortConfig.key];
        
        if (aValue < bValue) return sortConfig.direction === 'asc' ? -1 : 1;
        if (aValue > bValue) return sortConfig.direction === 'asc' ? 1 : -1;
        return 0;
      });
    }
    
    return filtered;
  }, [data, filters, sortConfig, searchTerm]);

  // Paginated data
  const paginatedData = useMemo(() => {
    const startIndex = (currentPage - 1) * pageSize;
    return filteredAndSortedData.slice(startIndex, startIndex + pageSize);
  }, [filteredAndSortedData, currentPage, pageSize]);

  // Handle sorting
  const handleSort = useCallback((key: string) => {
    setSortConfig(prevConfig => {
      if (prevConfig?.key === key) {
        return {
          key,
          direction: prevConfig.direction === 'asc' ? 'desc' : 'asc'
        };
      }
      return { key, direction: 'asc' };
    });
  }, []);

  // Handle filtering
  const handleFilterChange = useCallback((column: string, value: string, operator: string = 'contains') => {
    setFilters(prev => ({
      ...prev,
      [column]: {
        value,
        type: 'text' as const,
        operator: operator as FilterConfig[string]['operator']
      }
    }));
  }, []);

  // Handle cell selection
  const handleCellClick = useCallback((rowIndex: number, colKey: string) => {
    const cellKey = `${rowIndex}-${colKey}`;
    setSelectedCells(prev => {
      const newSet = new Set(prev);
      if (newSet.has(cellKey)) {
        newSet.delete(cellKey);
      } else {
        newSet.add(cellKey);
      }
      return newSet;
    });
  }, []);

  // Handle cell editing
  const handleCellDoubleClick = useCallback((rowIndex: number, colKey: string, currentValue: any) => {
    setEditingCell({ row: rowIndex, col: colKey });
    setEditValue(String(currentValue || ''));
  }, []);

  // Save cell edit
  const handleSaveEdit = useCallback(() => {
    if (editingCell && onDataChange) {
      const newData = [...data];
      const globalRowIndex = (currentPage - 1) * pageSize + editingCell.row;
      if (newData[globalRowIndex]) {
        newData[globalRowIndex][editingCell.col] = editValue;
        onDataChange(newData);
      }
    }
    setEditingCell(null);
    setEditValue('');
  }, [editingCell, editValue, data, onDataChange, currentPage, pageSize]);

  // Format cell value
  const formatCellValue = useCallback((value: any, column: string) => {
    if (value === null || value === undefined) return '';
    
    // Number formatting
    if (typeof value === 'number') {
      if (column.toLowerCase().includes('spread') || 
          column.toLowerCase().includes('score') ||
          column.toLowerCase().includes('bid') ||
          column.toLowerCase().includes('offer')) {
        return value.toFixed(2);
      }
      return value.toString();
    }
    
    return String(value);
  }, []);

  // Get cell class for styling
  const getCellClass = useCallback((value: any, column: string, rowIndex: number) => {
    const baseClass = 'excel-cell';
    const cellKey = `${rowIndex}-${column}`;
    
    let classes = [baseClass];
    
    if (selectedCells.has(cellKey)) {
      classes.push('selected');
    }
    
    if (typeof value === 'number') {
      classes.push('number');
      if (value > 0) classes.push('positive');
      if (value < 0) classes.push('negative');
    }
    
    return classes.join(' ');
  }, [selectedCells]);

  // Export to CSV
  const exportToCSV = useCallback(() => {
    const csvContent = [
      visibleColumns.join(','),
      ...filteredAndSortedData.map(row => 
        visibleColumns.map(col => `"${row[col] || ''}"`).join(',')
      )
    ].join('\n');
    
    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'bond_data.csv';
    a.click();
    URL.revokeObjectURL(url);
  }, [visibleColumns, filteredAndSortedData]);

  // Toggle column visibility
  const toggleColumnVisibility = useCallback((column: string) => {
    setHiddenColumns(prev => {
      const newSet = new Set(prev);
      if (newSet.has(column)) {
        newSet.delete(column);
      } else {
        newSet.add(column);
      }
      return newSet;
    });
  }, []);

  const totalPages = Math.ceil(filteredAndSortedData.length / pageSize);

  return (
    <div className="w-full h-full flex flex-col bg-white">
      {/* Toolbar */}
      <div className="flex items-center justify-between p-4 border-b bg-gray-50">
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2">
            <Search className="w-4 h-4 text-gray-500" />
            <input
              type="text"
              placeholder="Search all data..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="px-3 py-1 border rounded text-sm"
            />
          </div>
          
          <button
            onClick={exportToCSV}
            className="flex items-center gap-1 px-3 py-1 bg-blue-500 text-white rounded text-sm hover:bg-blue-600"
          >
            <Download className="w-4 h-4" />
            Export CSV
          </button>
          
          <div className="flex items-center gap-2">
            <Settings className="w-4 h-4 text-gray-500" />
            <select
              value={pageSize}
              onChange={(e) => setPageSize(Number(e.target.value))}
              className="px-2 py-1 border rounded text-sm"
            >
              <option value={25}>25 rows</option>
              <option value={50}>50 rows</option>
              <option value={100}>100 rows</option>
              <option value={500}>500 rows</option>
            </select>
          </div>
        </div>
        
        <div className="flex items-center gap-2">
          <span className="text-sm text-gray-600">
            Showing {((currentPage - 1) * pageSize) + 1} to {Math.min(currentPage * pageSize, filteredAndSortedData.length)} of {filteredAndSortedData.length} rows
          </span>
        </div>
      </div>

      {/* Column visibility controls */}
      <div className="p-2 border-b bg-gray-50">
        <div className="flex flex-wrap gap-2 max-h-20 overflow-y-auto">
          {columns.map(column => (
            <button
              key={column}
              onClick={() => toggleColumnVisibility(column)}
              className={`flex items-center gap-1 px-2 py-1 text-xs rounded ${
                hiddenColumns.has(column) 
                  ? 'bg-gray-200 text-gray-500' 
                  : 'bg-blue-100 text-blue-700'
              }`}
            >
              {hiddenColumns.has(column) ? <EyeOff className="w-3 h-3" /> : <Eye className="w-3 h-3" />}
              {column}
            </button>
          ))}
        </div>
      </div>

      {/* Table */}
      <div className="flex-1 overflow-auto">
        <table className="excel-table w-full">
          <thead>
            <tr>
              {visibleColumns.map(column => (
                <th
                  key={column}
                  className={`excel-header ${
                    sortConfig?.key === column 
                      ? sortConfig.direction === 'asc' ? 'sorted-asc' : 'sorted-desc'
                      : ''
                  }`}
                  onClick={() => handleSort(column)}
                >
                  <div className="flex items-center justify-between">
                    <span className="truncate">{column}</span>
                    <div className="flex flex-col ml-1">
                      <ChevronUp className="w-3 h-3 text-gray-400" />
                      <ChevronDown className="w-3 h-3 text-gray-400" />
                    </div>
                  </div>
                </th>
              ))}
            </tr>
            <tr>
              {visibleColumns.map(column => (
                <th key={`filter-${column}`} className="p-1 bg-gray-100">
                  <input
                    type="text"
                    placeholder="Filter..."
                    value={filters[column]?.value || ''}
                    onChange={(e) => handleFilterChange(column, e.target.value)}
                    className="w-full px-2 py-1 text-xs border rounded"
                  />
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {paginatedData.map((row, rowIndex) => (
              <tr key={rowIndex} className="excel-row">
                {visibleColumns.map(column => (
                  <td
                    key={`${rowIndex}-${column}`}
                    className={getCellClass(row[column], column, rowIndex)}
                    onClick={() => handleCellClick(rowIndex, column)}
                    onDoubleClick={() => handleCellDoubleClick(rowIndex, column, row[column])}
                  >
                    {editingCell?.row === rowIndex && editingCell?.col === column ? (
                      <input
                        type="text"
                        value={editValue}
                        onChange={(e) => setEditValue(e.target.value)}
                        onBlur={handleSaveEdit}
                        onKeyDown={(e) => {
                          if (e.key === 'Enter') handleSaveEdit();
                          if (e.key === 'Escape') setEditingCell(null);
                        }}
                        className="w-full bg-transparent outline-none"
                        autoFocus
                      />
                    ) : (
                      <span title={String(row[column] || '')}>
                        {formatCellValue(row[column], column)}
                      </span>
                    )}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      <div className="flex items-center justify-between p-4 border-t bg-gray-50">
        <div className="flex items-center gap-2">
          <button
            onClick={() => setCurrentPage(1)}
            disabled={currentPage === 1}
            className="px-3 py-1 border rounded text-sm disabled:opacity-50"
          >
            First
          </button>
          <button
            onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
            disabled={currentPage === 1}
            className="px-3 py-1 border rounded text-sm disabled:opacity-50"
          >
            Previous
          </button>
          <span className="text-sm">
            Page {currentPage} of {totalPages}
          </span>
          <button
            onClick={() => setCurrentPage(prev => Math.min(totalPages, prev + 1))}
            disabled={currentPage === totalPages}
            className="px-3 py-1 border rounded text-sm disabled:opacity-50"
          >
            Next
          </button>
          <button
            onClick={() => setCurrentPage(totalPages)}
            disabled={currentPage === totalPages}
            className="px-3 py-1 border rounded text-sm disabled:opacity-50"
          >
            Last
          </button>
        </div>
        
        <div className="flex items-center gap-2">
          <span className="text-sm text-gray-600">Go to page:</span>
          <input
            type="number"
            min={1}
            max={totalPages}
            value={currentPage}
            onChange={(e) => {
              const page = Number(e.target.value);
              if (page >= 1 && page <= totalPages) {
                setCurrentPage(page);
              }
            }}
            className="w-16 px-2 py-1 border rounded text-sm"
          />
        </div>
      </div>
    </div>
  );
};

export default ExcelTable;