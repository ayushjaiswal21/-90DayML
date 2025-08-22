import cv2
import numpy as np
import os

# --- Configuration ---
# IMPORTANT: Change these to paths to your actual image files.
# For best results, use an image with clear objects/shapes for Canny and morphology.
image_path_canny = 'sample.jpg'  # Using existing image
image_path_morphology = 'dog.jpg'  # Using existing image for morphology
output_dir = 'output_images_day63/'  # Directory to save processed images

# Create output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

print(f"OpenCV Version: {cv2.__version__}")
print(f"NumPy Version: {np.__version__}")

# Helper function to display images and wait for a key press
def display_and_save(window_name, image, filename=None, show_image=True):
    """
    Displays an image in a named window, waits for a key press,
    closes the window, and optionally saves the image.
    """
    if show_image:
        try:
            cv2.imshow(window_name, image)
            print(f"Displaying '{window_name}'. Press any key to close and continue...")
            cv2.waitKey(0)
            cv2.destroyAllWindows()  # Close all windows to avoid issues
        except Exception as e:
            print(f"Could not display image '{window_name}': {e}")
            print("Continuing without display...")
    else:
        print(f"Processing '{window_name}' (display disabled)")

    if filename:
        full_path = os.path.join(output_dir, filename)
        cv2.imwrite(full_path, image)
        print(f"  Saved: {full_path}")

# --- Section 1: Canny Edge Detection ---
print("\n--- Section 1: Canny Edge Detection ---")

# Load the image for Canny
img_canny = cv2.imread(image_path_canny)

if img_canny is None:
    print(f"Error: Could not load image from {image_path_canny} for Canny. Skipping this section.")
else:
    display_and_save('Original Image for Canny', img_canny.copy())

    # Convert to grayscale (Canny usually works best on grayscale)
    gray_canny = cv2.cvtColor(img_canny, cv2.COLOR_BGR2GRAY)
    display_and_save('Grayscale Image for Canny', gray_canny.copy(), 'canny_grayscale.jpg')

    # Apply Canny Edge Detector with different thresholds
    # Experiment with these values to see how they affect edge detection
    canny_params = [(50, 150), (100, 200), (150, 250)] # (threshold1, threshold2)

    for t1, t2 in canny_params:
        edges = cv2.Canny(gray_canny, t1, t2)
        window_name = f'Canny Edges (Thresholds: {t1}, {t2})'
        filename = f'canny_edges_{t1}_{t2}.jpg'
        display_and_save(window_name, edges, filename)

    # Optional: Combine original and edges for visual comparison
    # Ensure images have same number of channels for hstack
    edges_color = cv2.cvtColor(edges, cv2.COLOR_GRAY2BGR) # Convert edges back to BGR for stacking
    combined_canny = np.hstack((img_canny, edges_color))
    display_and_save('Original vs Canny Edges', combined_canny, 'canny_comparison.jpg')

print("\n--- Section 1: Canny Edge Detection Complete ---")

# --- Section 2: Morphological Operations ---
print("\n--- Section 2: Morphological Operations ---")

# Load the image for morphological operations
img_morph = cv2.imread(image_path_morphology)

if img_morph is None:
    print(f"Error: Could not load image from {image_path_morphology} for Morphology. Skipping this section.")
else:
    display_and_save('Original Image for Morphology', img_morph.copy())

    # Convert to grayscale
    gray_morph = cv2.cvtColor(img_morph, cv2.COLOR_BGR2GRAY)

    # Convert to binary image (thresholding is a common preprocessing step for morphology)
    # Pixels above 127 become 255 (white), below become 0 (black)
    _, binary_morph = cv2.threshold(gray_morph, 127, 255, cv2.THRESH_BINARY)
    display_and_save('Binary Image for Morphology', binary_morph.copy(), 'morph_binary.jpg')

    # Define a structuring element (kernel)
    # cv2.MORPH_RECT for a rectangular kernel
    # (5,5) means a 5x5 kernel
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (5, 5))
    print(f"\nUsing a {kernel.shape[0]}x{kernel.shape[1]} rectangular kernel for morphology:\n{kernel}")

    # 2.1 Erosion
    # Shrinks white regions (foreground) or expands black regions (background)
    eroded_img = cv2.erode(binary_morph, kernel, iterations=1)
    display_and_save('Eroded Image (1 Iteration)', eroded_img, 'eroded_image.jpg')

    eroded_img_2iter = cv2.erode(binary_morph, kernel, iterations=2)
    display_and_save('Eroded Image (2 Iterations)', eroded_img_2iter, 'eroded_image_2iter.jpg')

    # 2.2 Dilation
    # Expands white regions (foreground) or shrinks black regions (background)
    dilated_img = cv2.dilate(binary_morph, kernel, iterations=1)
    display_and_save('Dilated Image (1 Iteration)', dilated_img, 'dilated_image.jpg')

    dilated_img_2iter = cv2.dilate(binary_morph, kernel, iterations=2)
    display_and_save('Dilated Image (2 Iterations)', dilated_img_2iter, 'dilated_image_2iter.jpg')

    # 2.3 Opening (Erosion followed by Dilation)
    # Useful for removing small objects (noise) from the foreground
    opened_img = cv2.morphologyEx(binary_morph, cv2.MORPH_OPEN, kernel)
    display_and_save('Opened Image (Removes small white noise)', opened_img, 'opened_image.jpg')

    # 2.4 Closing (Dilation followed by Erosion)
    # Useful for filling small holes inside foreground objects or connecting nearby objects
    closed_img = cv2.morphologyEx(binary_morph, cv2.MORPH_CLOSE, kernel)
    display_and_save('Closed Image (Fills small holes/gaps)', closed_img, 'closed_image.jpg')

    # --- Compare all morphological operations side-by-side ---
    # Stack original binary, eroded, dilated
    row1_morph = np.hstack((binary_morph, eroded_img, dilated_img))
    # Stack opened, closed, and a blank for alignment
    row2_morph = np.hstack((opened_img, closed_img, np.zeros_like(binary_morph)))

    # Add labels (simple text overlay for demonstration)
    font = cv2.FONT_HERSHEY_SIMPLEX
    font_scale = 0.5
    font_thickness = 1
    text_color = (255, 255, 255) # White text on black background for visibility

    # Create a larger canvas to draw labels
    combined_morph_display = np.zeros((row1_morph.shape[0] * 2 + 50, row1_morph.shape[1], 3), dtype=np.uint8)
    combined_morph_display[:row1_morph.shape[0], :] = cv2.cvtColor(row1_morph, cv2.COLOR_GRAY2BGR)
    combined_morph_display[row1_morph.shape[0] + 50:, :] = cv2.cvtColor(row2_morph, cv2.COLOR_GRAY2BGR)

    cv2.putText(combined_morph_display, 'Binary Original', (10, 20), font, font_scale, text_color, font_thickness, cv2.LINE_AA)
    cv2.putText(combined_morph_display, 'Eroded', (binary_morph.shape[1] + 10, 20), font, font_scale, text_color, font_thickness, cv2.LINE_AA)
    cv2.putText(combined_morph_display, 'Dilated', (binary_morph.shape[1] * 2 + 10, 20), font, font_scale, text_color, font_thickness, cv2.LINE_AA)
    cv2.putText(combined_morph_display, 'Opened', (10, binary_morph.shape[0] + 70), font, font_scale, text_color, font_thickness, cv2.LINE_AA)
    cv2.putText(combined_morph_display, 'Closed', (binary_morph.shape[1] + 10, binary_morph.shape[0] + 70), font, font_scale, text_color, font_thickness, cv2.LINE_AA)

    display_and_save('Morphological Operations Comparison', combined_morph_display, 'morphology_comparison.jpg')

print("\n--- Section 2: Morphological Operations Complete ---")
print("\nAll Day 63 tasks completed!")
