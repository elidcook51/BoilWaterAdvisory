import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import itertools


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

# #-----------------Start of NA dates values --------------------
# import matplotlib.pyplot as plt

# # Create pattern counts (your existing logic)
# pattern = BWAvirginia[['start_date', 'end_date', 'backup_date']].notna().astype(int)
# pattern_tuples = pattern.apply(tuple, axis=1)
# pattern_counts = pattern_tuples.value_counts()

# label_map = {
#     (0,0,0): 'No Dates',
#     (1,0,0): 'Start Only',
#     (0,1,0): 'End Only',
#     (0,0,1): 'Backup Only',
#     (1,1,0): 'Start + End',
#     (1,0,1): 'Start + Backup',
#     (0,1,1): 'End + Backup',
#     (1,1,1): 'All Dates'
# }

# pattern_counts.index = pattern_counts.index.map(label_map)

# # Sort for cleaner visual flow
# pattern_counts = pattern_counts.sort_values(ascending=False)

# # Theme colors (primary blue + orange accent)
# colors = ['#4C7899' if label != 'No Dates' else '#F28C28' for label in pattern_counts.index]

# # Create figure
# plt.figure(figsize=(7, 5))

# bars = plt.bar(pattern_counts.index, pattern_counts.values, color=colors)

# # Add value labels on top of bars
# for bar in bars:
#     height = bar.get_height()
#     plt.text(
#         bar.get_x() + bar.get_width() / 2,
#         height,
#         f'{int(height)}',
#         ha='center',
#         va='bottom',
#         fontsize=10,
#         color='#333333'
#     )

# # Style axes and title
# plt.title(
#     'Availability of Date Information in Advisories',
#     fontsize=14,
#     color='#4C7899',
#     pad=15
# )

# plt.ylabel('Count', fontsize=11, color='#333333')
# plt.xlabel('Date Information Type', fontsize=11, color='#333333')

# plt.xticks(rotation=30, ha='right', fontsize=10)
# plt.yticks(fontsize=10)

# # Remove top/right spines for cleaner look
# ax = plt.gca()
# ax.spines['top'].set_visible(False)
# ax.spines['right'].set_visible(False)

# # Light grid for readability
# plt.grid(axis='y', linestyle='--', alpha=0.4)

# plt.tight_layout()
# plt.savefig("C:/Users/ucg8nb/Downloads/How many dates.png")

# --------------------- End of NA date values -----------------


# #-----------------Start of NA location values --------------------
# # Create pattern counts (your existing logic)
# pattern = BWAvirginia[['location_state', 'location_county', 'location_locality']].notna().astype(int)
# pattern_tuples = pattern.apply(tuple, axis=1)
# pattern_counts = pattern_tuples.value_counts()

# all_patterns = list(itertools.product([0,1], repeat = 3))

# pattern_counts = pattern_counts.reindex(all_patterns, fill_value = 0)

# label_map = {
#     (0,0,0): 'No Location',
#     (1,0,0): 'State Only',
#     (0,1,0): 'County Only',
#     (0,0,1): 'Locality Only',
#     (1,1,0): 'State + County',
#     (1,0,1): 'State + Locality',
#     (0,1,1): 'County + Locality',
#     (1,1,1): 'All Location'
# }

# labels = [label_map[p] for p in pattern_counts.index]

# import matplotlib.pyplot as plt
# import numpy as np

# # Function to hide tiny percentages
# def autopct_format(pct):
#     return f'{pct:.1f}%' if pct > 3 else ''

# values = pattern_counts.values

# # Gradient colors
# cmap = plt.cm.Blues
# colors = cmap(np.linspace(0.35, 0.95, len(values)))

# plt.figure(figsize=(8, 8))

# wedges, texts, autotexts = plt.pie(
#     values,
#     labels=None,  # ✅ remove labels from slices
#     colors=colors,
#     startangle=90,
#     autopct=autopct_format,
#     pctdistance=0.7,
#     wedgeprops=dict(edgecolor='white', linewidth=1)
# )

# # ✅ Add legend instead
# plt.legend(
#     wedges,
#     labels,
#     title="Location Type",
#     loc="center left",
#     bbox_to_anchor=(1, 0.5),  # move legend outside
#     fontsize=10,
#     title_fontsize=11
# )

# plt.title(
#     'Availability of Location Information in Advisories',
#     fontsize=14,
#     color='#4C7899',
#     pad=20
# )

# plt.tight_layout()
# plt.savefig("C:/Users/ucg8nb/Downloads/location_pie_legend.png")
# plt.show()

# # ---------------------- LLM accuracy date -------------------------
# import matplotlib.pyplot as plt

# # ----- Data construction -----
# # Total = 50
# no_info = 6

# vt_advisory = 4
# other_advisory = 2
# bad_date_total = vt_advisory + other_advisory  # 6

# remaining = 50 - no_info - bad_date_total  # 38

# # Values for wedges
# values = [
#     remaining,
#     vt_advisory,
#     other_advisory,
#     no_info
# ]

# # Labels
# labels = [
#     'Good Date',
#     'Bad Date (VT Advisory)',
#     'Bad Date (Other Advisory)',
#     'No Information'
# ]

# # ----- Colors -----
# # Match your previous palette:
# # - Blues gradient for "good"
# # - Orange for emphasis (bad)
# # - Gray for missing

# colors = [
#     '#4C7899',      # main blue (good data)
#     '#F28C28',      # orange (VT advisory)
#     '#f6b26b',      # lighter orange (other advisory)
#     '#B0B0B0'       # gray (no info)
# ]

# # ----- Plot -----
# plt.figure(figsize=(7, 7))

# wedges, texts, autotexts = plt.pie(
#     values,
#     labels=None,  # use legend instead
#     colors=colors,
#     startangle=90,
#     autopct='%1.1f%%',
#     pctdistance=0.7,
#     wedgeprops=dict(edgecolor='white', linewidth=1)
# )

# # ----- Legend -----
# plt.legend(
#     wedges,
#     labels,
#     title="Category",
#     loc='center left',
#     bbox_to_anchor=(1, 0.5),
#     fontsize=10,
#     title_fontsize=11
# )

# # ----- Title -----
# plt.title(
#     'Date Quality Classification (n = 50)',
#     fontsize=14,
#     color='#4C7899',
#     pad=15
# )

# plt.tight_layout()
# plt.savefig("C:/Users/ucg8nb/Downloads/date_quality_pie.png")
# plt.show()
# #----------------------------------End of llm stuff --------------------

llm_accuracy_path = "C:/Users/ucg8nb/Downloads/LLM Accuracy Tracker Virginia.csv"

llm_accuracy_df = pd.read_csv(llm_accuracy_path)

df = llm_accuracy_df[['location_state', 'location_county', 'location_locality']]
df = df.rename(columns = {'location_state': "State", 'location_county': 'County', 'location_locality': "Locality"})

# Ensure desired column order
ordered_cols = ['State', 'County', 'Locality']
means = df[ordered_cols].mean()

# Theme colors (match previous plots)
bar_colors = ['#4C7899', '#3A6D8C', '#2F5F7A']  # subtle blue gradient

plt.figure(figsize=(6, 5))

bars = plt.bar(
    means.index,
    means.values,
    color=bar_colors,
    edgecolor='white',
    linewidth=1
)

plt.ylim(0,1)
plt.ylabel("Proportion Correct")
plt.title("Proportion Correct by Location Type")
plt.savefig("C:/Users/ucg8nb/Downloads/LLM accuracy location.png")