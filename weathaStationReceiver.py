import sys
import json


def main():
    for line in sys.stdin:
        try:
            # Parse the JSON message from the input line
            msg = json.loads(line.strip())

            # Check if the model is "AmbientWeather-WH31E"
            if msg.get("model") == "AmbientWeather-WH31E":
                # Extract the required fields
                sensor_id = msg.get("id")
                humidity = msg.get("humidity")
                temperature = msg.get("temperature_C")

                # Print the values
                print(f"id: {sensor_id}, humidity: {humidity}%, temperature: {temperature}°C")
        except json.JSONDecodeError:
            print("Invalid JSON format", file=sys.stderr)
        except Exception as e:
            print(f"Error processing message: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
