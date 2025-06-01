"""
Verify the NaN bug in negative value checking.
"""
import pandas as pd
import numpy as np

print("=== VERIFYING NaN BUG ===")

# Create test data
test_data = pd.DataFrame({
    'value': [1.0, -1.0, 0.0, np.nan, 5.0]
})

print("\n1. Test data:")
print(test_data)

print("\n2. Checking value >= 0:")
result = test_data['value'] >= 0
print(result)

print("\n3. Filtering with value >= 0:")
filtered = test_data[test_data['value'] >= 0]
print(filtered)
print(f"\nRows lost: {len(test_data) - len(filtered)}")
print("Note: NaN row was removed!")

print("\n4. Correct way to handle this:")
# Option 1: Explicitly check for negative values
mask1 = ~(test_data['value'] < 0)  # This keeps NaN
filtered1 = test_data[mask1]
print("Option 1 - Using ~(value < 0):")
print(filtered1)

# Option 2: Handle NaN separately
mask2 = (test_data['value'] >= 0) | test_data['value'].isna()
filtered2 = test_data[mask2]
print("\nOption 2 - Using (value >= 0) | isna():")
print(filtered2)

print("\n=== CONCLUSION ===")
print("The bug is that 'value >= 0' returns False for NaN values,")
print("causing rows with NaN to be incorrectly filtered out when")
print("the intention is only to remove negative values.")

print("\n=== END VERIFICATION ===") 