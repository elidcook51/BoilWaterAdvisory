import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


BWAvirginia = pd.read_csv("C:/Users/ucg8nb/Downloads/Virginia Run.csv")

justUseless = BWAvirginia[['start_date', 'end_date', 'backup_date', 'location_state', 'location_county', 'location_locality']].isna().all(axis = 1)

columns = ['start_date', 'end_date', 'backup_date', 'location_state', 'location_county', 'location_locality']

BWAvirginia = BWAvirginia.dropna(subset = columns, how = 'all')

#------------Start of pie chart by states -------------------
# # Get counts
# counts = BWAvirginia['location_state'].value_counts()

# # Keep top N states, group the rest
# top_n = 5
# top_counts = counts[:top_n]
# other_sum = counts[top_n:].sum()

# # Combine into one series
# plot_data = top_counts.copy()
# plot_data['Other'] = other_sum

# # Colors (theme aligned)
# colors = ['#4C7899', '#F28C28', '#7FA6C9', '#AFC4D6', '#D6E2EC', '#B0B0B0']

# # Create plot
# plt.figure(figsize=(6, 6))

# wedges, texts, autotexts = plt.pie(
#     plot_data,
#     labels=plot_data.index,
#     autopct=lambda p: f'{p:.1f}%' if p > 3 else '',  # Hide tiny % labels
#     startangle=140,
#     colors=colors[:len(plot_data)],
#     wedgeprops={'edgecolor': 'white', 'linewidth': 1.5}
# )

# # Style percentages

# for autotext in autotexts:
#     autotext.set_color('white')
#     autotext.set_weight('bold')
#     autotext.set_fontsize(10)

# # Title
# plt.title(
#     'Distribution of Advisories by State',
#     fontsize=14,
#     color='#4C7899',
#     pad=15
# )

# plt.tight_layout()

# plt.savefig("C:/Users/ucg8nb/Downloads/Distribution of Seperate States.png")
#------------End of pie chart by states -------------------

#-----------------Start of NA dates values --------------------
import matplotlib.pyplot as plt

# Create pattern counts (your existing logic)
pattern = BWAvirginia[['start_date', 'end_date', 'backup_date']].notna().astype(int)
pattern_tuples = pattern.apply(tuple, axis=1)
pattern_counts = pattern_tuples.value_counts()

label_map = {
    (0,0,0): 'No Dates',
    (1,0,0): 'Start Only',
    (0,1,0): 'End Only',
    (0,0,1): 'Backup Only',
    (1,1,0): 'Start + End',
    (1,0,1): 'Start + Backup',
    (0,1,1): 'End + Backup',
    (1,1,1): 'All Dates'
}

pattern_counts.index = pattern_counts.index.map(label_map)

# Sort for cleaner visual flow
pattern_counts = pattern_counts.sort_values(ascending=False)

# Theme colors (primary blue + orange accent)
colors = ['#4C7899' if label != 'No Dates' else '#F28C28' for label in pattern_counts.index]

# Create figure
plt.figure(figsize=(7, 5))

bars = plt.bar(pattern_counts.index, pattern_counts.values, color=colors)

# Add value labels on top of bars
for bar in bars:
    height = bar.get_height()
    plt.text(
        bar.get_x() + bar.get_width() / 2,
        height,
        f'{int(height)}',
        ha='center',
        va='bottom',
        fontsize=10,
        color='#333333'
    )

# Style axes and title
plt.title(
    'Availability of Date Information in Advisories',
    fontsize=14,
    color='#4C7899',
    pad=15
)

plt.ylabel('Count', fontsize=11, color='#333333')
plt.xlabel('Date Information Type', fontsize=11, color='#333333')

plt.xticks(rotation=30, ha='right', fontsize=10)
plt.yticks(fontsize=10)

# Remove top/right spines for cleaner look
ax = plt.gca()
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

# Light grid for readability
plt.grid(axis='y', linestyle='--', alpha=0.4)

plt.tight_layout()
plt.savefig("C:/Users/ucg8nb/Downloads/How many dates.png")