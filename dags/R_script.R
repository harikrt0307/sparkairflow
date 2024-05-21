# Load necessary library
install.packages("ggplot2")
library(ggplot2)

# Create a simple data frame
data <- data.frame(
  ID = 1:4,
  Name = c("Alice", "Bob", "Cathy", "David"),
  Age = c(29, 31, 25, 35)
)

# Print the data frame
print("Original Data Frame:")
print(data)

# Select specific columns
selected_data <- data[, c("Name", "Age")]
print("Selected Columns (Name and Age):")
print(selected_data)

# Filter rows where Age is greater than 30
filtered_data <- subset(data, Age > 30)
print("Filtered Rows (Age > 30):")
print(filtered_data)

# Group by Age and count the number of occurrences
age_counts <- aggregate(ID ~ Age, data, length)
names(age_counts)[2] <- "Count"
print("Age Group Counts:")
print(age_counts)

# Create a simple plot using ggplot2
ggplot(data, aes(x = Name, y = Age)) +
  geom_bar(stat = "identity", fill = "steelblue") +
  labs(title = "Age of Individuals", x = "Name", y = "Age") +
  theme_minimal()
