# A script to generate test data to verify your C++ logic
def generate_expected_data():
    with open("expected_output.txt", "w") as f:
        for i in range(100000):
            f.write(f"key{i}:value{i}\n")

if __name__ == "__main__":
    generate_expected_data()
    print("Generated expected test data for comparison.")
