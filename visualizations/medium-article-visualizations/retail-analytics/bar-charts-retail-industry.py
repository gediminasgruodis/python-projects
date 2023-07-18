#!/usr/bin/env python
# coding: utf-8

# In[5]:


import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

# Sample data for the chart
areas_of_impact = ['Customer Personalization', 'Inventory Management and Supply Chain', 'Pricing Strategies', 'Operational Efficiency', 'Customer Insights and Market Trends']
level_of_impact = [3, 4, 5, 2, 4]

# Normalize the impact values to the range [0, 1]
normalized_impact = np.array(level_of_impact) / max(level_of_impact)

# Creating a DataFrame from the data
data = {'Areas of Impact': areas_of_impact, 'Level of Impact': level_of_impact, 'Normalized Impact': normalized_impact}
df = pd.DataFrame(data)

# Plotting the bar chart using Seaborn with conditional formatting
sns.set(style='whitegrid')
plt.figure(figsize=(8, 6))

# Sort the DataFrame in descending order by the impact level
df.sort_values(by='Level of Impact', ascending=False, inplace=True)

# Assign lower transparency (higher alpha) for higher impact values
colors = sns.color_palette("Blues_r", len(df))
colors = [(r, g, b, 1 - impact) for (r, g, b), impact in zip(colors, df['Normalized Impact'])]

sns.barplot(x='Level of Impact', y='Areas of Impact', data=df, palette=colors)

plt.xlabel('Level of Impact')
plt.ylabel('Areas of Impact')
plt.title('Impact of Data Analytics on the Retail Industry')

plt.show()


# In[ ]:




