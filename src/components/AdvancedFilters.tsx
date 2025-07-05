import React, { useState, useCallback } from 'react';
import { Filter, X, Plus, Trash2, Save, RotateCcw } from 'lucide-react';

interface FilterCriteria {
  id: string;
  column: string;
  operator: 'equals' | 'contains' | 'greater' | 'less' | 'between' | 'in';
  value: string;
  value2?: string; // For 'between' operator
  active: boolean;
}

interface PresetFilter {
  id: string;
  name: string;
  criteria: FilterCriteria[];
}

interface AdvancedFiltersProps {
  columns: string[];
  onFiltersChange: (filters: FilterCriteria[]) => void;
  data: any[];
}

const AdvancedFilters: React.FC<AdvancedFiltersProps> = ({ 
  columns, 
  onFiltersChange, 
  data 
}) => {
  const [isOpen, setIsOpen] = useState(false);
  const [filters, setFilters] = useState<FilterCriteria[]>([]);
  const [presets, setPresets] = useState<PresetFilter[]>([
    {
      id: 'high-spread',
      name: 'High Spread Bonds',
      criteria: [
        {
          id: '1',
          column: 'Last_Spread',
          operator: 'greater',
          value: '100',
          active: true
        }
      ]
    },
    {
      id: 'investment-grade',
      name: 'Investment Grade',
      criteria: [
        {
          id: '2',
          column: 'Rating_1',
          operator: 'in',
          value: 'AAA,AA,A,BBB',
          active: true
        }
      ]
    },
    {
      id: 'high-zscore',
      name: 'High Z-Score (>2)',
      criteria: [
        {
          id: '3',
          column: 'Z_Score',
          operator: 'greater',
          value: '2',
          active: true
        }
      ]
    }
  ]);
  const [newPresetName, setNewPresetName] = useState('');

  const addFilter = useCallback(() => {
    const newFilter: FilterCriteria = {
      id: Date.now().toString(),
      column: columns[0] || '',
      operator: 'contains',
      value: '',
      active: true
    };
    setFilters(prev => [...prev, newFilter]);
  }, [columns]);

  const updateFilter = useCallback((id: string, updates: Partial<FilterCriteria>) => {
    setFilters(prev => prev.map(filter => 
      filter.id === id ? { ...filter, ...updates } : filter
    ));
  }, []);

  const removeFilter = useCallback((id: string) => {
    setFilters(prev => prev.filter(filter => filter.id !== id));
  }, []);

  const applyFilters = useCallback(() => {
    const activeFilters = filters.filter(f => f.active && f.value);
    onFiltersChange(activeFilters);
  }, [filters, onFiltersChange]);

  const clearAllFilters = useCallback(() => {
    setFilters([]);
    onFiltersChange([]);
  }, [onFiltersChange]);

  const saveAsPreset = useCallback(() => {
    if (newPresetName && filters.length > 0) {
      const newPreset: PresetFilter = {
        id: Date.now().toString(),
        name: newPresetName,
        criteria: [...filters]
      };
      setPresets(prev => [...prev, newPreset]);
      setNewPresetName('');
    }
  }, [newPresetName, filters]);

  const loadPreset = useCallback((preset: PresetFilter) => {
    setFilters(preset.criteria);
    onFiltersChange(preset.criteria.filter(f => f.active));
  }, [onFiltersChange]);

  const getOperatorOptions = (column: string) => {
    const numericColumns = ['Last_Spread', 'Z_Score', 'Max', 'Min', 'Percentile', 'Best Bid_1', 'Best Offer_1'];
    const isNumeric = numericColumns.some(col => column.includes(col));
    
    if (isNumeric) {
      return [
        { value: 'equals', label: 'Equals' },
        { value: 'greater', label: 'Greater than' },
        { value: 'less', label: 'Less than' },
        { value: 'between', label: 'Between' }
      ];
    }
    
    return [
      { value: 'equals', label: 'Equals' },
      { value: 'contains', label: 'Contains' },
      { value: 'in', label: 'In list' }
    ];
  };

  const getUniqueValues = (column: string) => {
    if (!data || data.length === 0) return [];
    const values = [...new Set(data.map(row => row[column]).filter(Boolean))];
    return values.slice(0, 10); // Limit to first 10 unique values
  };

  if (!isOpen) {
    return (
      <button
        onClick={() => setIsOpen(true)}
        className="flex items-center gap-2 px-3 py-1 bg-purple-500 text-white rounded text-sm hover:bg-purple-600"
      >
        <Filter className="w-4 h-4" />
        Advanced Filters
      </button>
    );
  }

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-4xl max-h-[90vh] overflow-auto">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b">
          <h2 className="text-xl font-semibold">Advanced Filters</h2>
          <button
            onClick={() => setIsOpen(false)}
            className="p-1 hover:bg-gray-100 rounded"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Preset Filters */}
        <div className="p-4 border-b bg-gray-50">
          <h3 className="font-medium mb-3">Preset Filters</h3>
          <div className="flex flex-wrap gap-2 mb-3">
            {presets.map(preset => (
              <button
                key={preset.id}
                onClick={() => loadPreset(preset)}
                className="px-3 py-1 bg-blue-100 text-blue-700 rounded text-sm hover:bg-blue-200"
              >
                {preset.name}
              </button>
            ))}
          </div>
          
          {/* Save new preset */}
          <div className="flex items-center gap-2">
            <input
              type="text"
              placeholder="Preset name..."
              value={newPresetName}
              onChange={(e) => setNewPresetName(e.target.value)}
              className="px-3 py-1 border rounded text-sm flex-1"
            />
            <button
              onClick={saveAsPreset}
              disabled={!newPresetName || filters.length === 0}
              className="flex items-center gap-1 px-3 py-1 bg-green-500 text-white rounded text-sm hover:bg-green-600 disabled:opacity-50"
            >
              <Save className="w-4 h-4" />
              Save
            </button>
          </div>
        </div>

        {/* Filter Criteria */}
        <div className="p-4">
          <div className="flex items-center justify-between mb-4">
            <h3 className="font-medium">Filter Criteria</h3>
            <div className="flex gap-2">
              <button
                onClick={addFilter}
                className="flex items-center gap-1 px-3 py-1 bg-blue-500 text-white rounded text-sm hover:bg-blue-600"
              >
                <Plus className="w-4 h-4" />
                Add Filter
              </button>
              <button
                onClick={clearAllFilters}
                className="flex items-center gap-1 px-3 py-1 bg-gray-500 text-white rounded text-sm hover:bg-gray-600"
              >
                <RotateCcw className="w-4 h-4" />
                Clear All
              </button>
            </div>
          </div>

          {filters.length === 0 && (
            <div className="text-center py-8 text-gray-500">
              No filters defined. Click "Add Filter" to get started.
            </div>
          )}

          {filters.map((filter, index) => (
            <div key={filter.id} className="flex items-center gap-3 mb-3 p-3 border rounded">
              <input
                type="checkbox"
                checked={filter.active}
                onChange={(e) => updateFilter(filter.id, { active: e.target.checked })}
                className="w-4 h-4"
              />
              
              {index > 0 && (
                <span className="text-sm font-medium text-gray-500">AND</span>
              )}
              
              <select
                value={filter.column}
                onChange={(e) => updateFilter(filter.id, { column: e.target.value })}
                className="px-3 py-1 border rounded text-sm"
              >
                {columns.map(col => (
                  <option key={col} value={col}>{col}</option>
                ))}
              </select>
              
              <select
                value={filter.operator}
                onChange={(e) => updateFilter(filter.id, { operator: e.target.value as any })}
                className="px-3 py-1 border rounded text-sm"
              >
                {getOperatorOptions(filter.column).map(op => (
                  <option key={op.value} value={op.value}>{op.label}</option>
                ))}
              </select>
              
              <input
                type="text"
                placeholder="Value..."
                value={filter.value}
                onChange={(e) => updateFilter(filter.id, { value: e.target.value })}
                className="px-3 py-1 border rounded text-sm flex-1"
                list={`values-${filter.id}`}
              />
              
              <datalist id={`values-${filter.id}`}>
                {getUniqueValues(filter.column).map((value, i) => (
                  <option key={i} value={String(value)} />
                ))}
              </datalist>
              
              {filter.operator === 'between' && (
                <input
                  type="text"
                  placeholder="To..."
                  value={filter.value2 || ''}
                  onChange={(e) => updateFilter(filter.id, { value2: e.target.value })}
                  className="px-3 py-1 border rounded text-sm w-24"
                />
              )}
              
              <button
                onClick={() => removeFilter(filter.id)}
                className="p-1 text-red-500 hover:bg-red-100 rounded"
              >
                <Trash2 className="w-4 h-4" />
              </button>
            </div>
          ))}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between p-4 border-t bg-gray-50">
          <div className="text-sm text-gray-600">
            {filters.filter(f => f.active).length} active filters
          </div>
          <div className="flex gap-2">
            <button
              onClick={() => setIsOpen(false)}
              className="px-4 py-2 border rounded hover:bg-gray-100"
            >
              Cancel
            </button>
            <button
              onClick={() => {
                applyFilters();
                setIsOpen(false);
              }}
              className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
            >
              Apply Filters
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default AdvancedFilters;