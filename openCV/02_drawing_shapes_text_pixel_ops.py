import cv2
import numpy as np

def drawing_shapes_practice():
    """Practice drawing shapes on a blank image"""
    print("=== Drawing Shapes Practice ===")
    
    # Create a blank black image (500x500 pixels, 3 channels for BGR)
    img = np.zeros((500, 500, 3), dtype=np.uint8)
    
    # Draw a blue line from (50, 50) to (450, 450)
    cv2.line(img, (50, 50), (450, 450), (255, 0, 0), 3)  # Blue line, thickness 3
    
    # Draw another line - diagonal from top-right to bottom-left
    cv2.line(img, (450, 50), (50, 450), (0, 255, 255), 2)  # Yellow line, thickness 2
    
    # Draw a green rectangle (outline)
    cv2.rectangle(img, (100, 100), (200, 150), (0, 255, 0), 2)  # Green outline, thickness 2
    
    # Draw a filled green rectangle
    cv2.rectangle(img, (250, 100), (350, 150), (0, 255, 0), -1)  # Filled green rectangle
    
    # Draw a red circle (outline)
    cv2.circle(img, (150, 250), 50, (0, 0, 255), 3)  # Red circle outline, radius 50, thickness 3
    
    # Draw a filled red circle
    cv2.circle(img, (350, 250), 40, (0, 0, 255), -1)  # Filled red circle, radius 40
    
    # Experiment with different colors and thicknesses
    cv2.circle(img, (250, 350), 30, (255, 255, 0), 5)  # Cyan circle, thick outline
    cv2.rectangle(img, (50, 300), (120, 370), (255, 0, 255), 4)  # Magenta rectangle
    
    cv2.imshow('Drawing Shapes Practice', img)
    cv2.waitKey(0)
    cv2.destroyAllWindows()
    
    return img

def drawing_text_practice():
    """Practice adding text to images"""
    print("=== Drawing Text Practice ===")
    
    # Create a blank white image for better text visibility
    img = np.ones((400, 600, 3), dtype=np.uint8) * 255
    
    # Add your name (replace with your actual name)
    cv2.putText(img, 'OpenCV Learning Journey', (50, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 0), 2)
    
    # Try different fonts
    cv2.putText(img, 'FONT_HERSHEY_SIMPLEX', (50, 100), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 0, 0), 2)
    cv2.putText(img, 'FONT_HERSHEY_COMPLEX', (50, 130), cv2.FONT_HERSHEY_COMPLEX, 0.7, (0, 255, 0), 2)
    cv2.putText(img, 'FONT_HERSHEY_SCRIPT_SIMPLEX', (50, 160), cv2.FONT_HERSHEY_SCRIPT_SIMPLEX, 0.7, (0, 0, 255), 2)
    
    # Different scales
    cv2.putText(img, 'Small Text', (50, 200), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (128, 128, 128), 1)
    cv2.putText(img, 'Medium Text', (50, 230), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (64, 64, 64), 2)
    cv2.putText(img, 'Large Text', (50, 270), cv2.FONT_HERSHEY_SIMPLEX, 1.2, (0, 0, 0), 3)
    
    # Anti-aliased text (smoother)
    cv2.putText(img, 'Anti-aliased Text', (50, 320), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 128, 0), 2, cv2.LINE_AA)
    
    cv2.imshow('Text Practice', img)
    cv2.waitKey(0)
    cv2.destroyAllWindows()
    
    return img

def add_text_to_existing_image():
    """Add text to an existing image"""
    print("=== Adding Text to Existing Image ===")
    
    try:
        # Try to load an image (you can replace with any image path)
        img_paths = [
            r'C:\Users\pc\Desktop\90DayML\openCV\cat_PNG118.png',
            r'C:\Users\pc\Desktop\90DayML\openCV\cat-cat-meme.jpg',
            r'C:\Users\pc\Desktop\90DayML\openCV\dog.jpg'
        ]
        
        img = None
        for path in img_paths:
            try:
                img = cv2.imread(path)
                if img is not None:
                    print(f"Successfully loaded: {path}")
                    break
            except:
                continue
        
        if img is None:
            print("No images found, creating a sample image")
            img = np.random.randint(0, 256, (300, 400, 3), dtype=np.uint8)
        
        # Add label text
        cv2.putText(img, 'Labeled Image', (10, 30), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2, cv2.LINE_AA)
        cv2.putText(img, 'Region of Interest', (10, img.shape[0] - 20), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 255), 2)
        
        cv2.imshow('Text on Existing Image', img)
        cv2.waitKey(0)
        cv2.destroyAllWindows()
        
    except Exception as e:
        print(f"Error loading image: {e}")

def pixel_operations_practice():
    """Practice direct pixel access and manipulation"""
    print("=== Pixel Operations Practice ===")
    
    # Load an image or create a sample one
    try:
        img = cv2.imread(r'C:\Users\pc\Desktop\90DayML\openCV\cat_PNG118.png')
        if img is None:
            # Create a sample image if no image is found
            img = np.random.randint(0, 256, (200, 200, 3), dtype=np.uint8)
            print("Created sample image for pixel operations")
        else:
            print("Using loaded image for pixel operations")
    except:
        img = np.random.randint(0, 256, (200, 200, 3), dtype=np.uint8)
        print("Created sample image for pixel operations")
    
    # Make a copy to preserve original
    original = img.copy()
    
    # Manually change individual pixels
    img[50, 50] = [0, 0, 255]  # Red pixel
    img[51, 50] = [0, 255, 0]  # Green pixel
    img[52, 50] = [255, 0, 0]  # Blue pixel
    
    # Change a 5x5 block using slicing
    img[100:105, 100:105] = [255, 255, 0]  # Cyan 5x5 block
    
    # Change a 5x5 block using iteration (alternative method)
    for y in range(120, 125):
        for x in range(120, 125):
            img[y, x] = [255, 0, 255]  # Magenta 5x5 block
    
    # Display original and modified
    combined = np.hstack((original, img))
    cv2.imshow('Pixel Operations: Original vs Modified', combined)
    cv2.waitKey(0)
    cv2.destroyAllWindows()
    
    return img

def brightness_adjustment_practice():
    """Practice adjusting brightness using OpenCV functions"""
    print("=== Brightness Adjustment Practice ===")
    
    # Create or load an image
    try:
        img = cv2.imread(r'C:\Users\pc\Desktop\90DayML\openCV\cat_PNG118.png')
        if img is None:
            img = np.random.randint(50, 200, (200, 300, 3), dtype=np.uint8)
            print("Created sample image for brightness adjustment")
    except:
        img = np.random.randint(50, 200, (200, 300, 3), dtype=np.uint8)
        print("Created sample image for brightness adjustment")
    
    # Brighten image using cv2.add (safe, handles clipping)
    bright_img = cv2.add(img, np.array([50, 50, 50]))
    
    # Darken image using cv2.subtract (safe, handles clipping)
    dark_img = cv2.subtract(img, np.array([30, 30, 30]))
    
    # Demonstrate the difference between OpenCV and NumPy operations
    # WARNING: NumPy addition can cause wrap-around (don't use for brightness)
    numpy_add = img + 50  # This can cause wrap-around issues
    
    # Display all versions
    row1 = np.hstack((img, bright_img))
    row2 = np.hstack((dark_img, numpy_add))
    combined = np.vstack((row1, row2))
    
    # Add labels (this is just for demonstration - normally you'd use separate windows)
    cv2.putText(combined, 'Original', (10, 20), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
    cv2.putText(combined, 'cv2.add +50', (img.shape[1] + 10, 20), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
    cv2.putText(combined, 'cv2.subtract -30', (10, img.shape[0] + 20), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
    cv2.putText(combined, 'NumPy +50 (wrap)', (img.shape[1] + 10, img.shape[0] + 20), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
    
    cv2.imshow('Brightness Adjustment Comparison', combined)
    cv2.waitKey(0)
    cv2.destroyAllWindows()

def image_arithmetic_practice():
    """Practice image arithmetic operations"""
    print("=== Image Arithmetic Practice ===")
    
    # Create two simple images
    img1 = np.zeros((300, 300, 3), dtype=np.uint8)
    img2 = np.zeros((300, 300, 3), dtype=np.uint8)
    
    # Add a white square to img1
    cv2.rectangle(img1, (50, 50), (150, 150), (255, 255, 255), -1)
    
    # Add a white circle to img2
    cv2.circle(img2, (200, 200), 50, (255, 255, 255), -1)
    
    # Add the images
    added = cv2.add(img1, img2)
    
    # Subtract img2 from img1
    subtracted = cv2.subtract(img1, img2)
    
    # Display the results
    row1 = np.hstack((img1, img2))
    row2 = np.hstack((added, subtracted))
    combined = np.vstack((row1, row2))
    
    cv2.putText(combined, 'Image 1 (Square)', (10, 20), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
    cv2.putText(combined, 'Image 2 (Circle)', (310, 20), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
    cv2.putText(combined, 'cv2.add(img1, img2)', (10, 320), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
    cv2.putText(combined, 'cv2.subtract(img1, img2)', (310, 320), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
    
    cv2.imshow('Image Arithmetic: Add and Subtract', combined)
    cv2.waitKey(0)
    cv2.destroyAllWindows()

def image_blending_practice():
    """Practice blending images with cv2.addWeighted"""
    print("=== Image Blending Practice ===")
    
    try:
        # Try to load the specified images
        img1_path = r'C:\Users\pc\Desktop\90DayML\openCV\cat_PNG118.png'
        img2_path = r'C:\Users\pc\Desktop\90DayML\openCV\cat-cat-meme.jpg'
        img3_path = r'C:\Users\pc\Desktop\90DayML\openCV\dog.jpg'
        
        img1 = cv2.imread(img1_path)
        img2 = cv2.imread(img2_path)
        img3 = cv2.imread(img3_path)
        
        # Check which images loaded successfully
        available_images = []
        if img1 is not None:
            available_images.append(('Cat PNG', img1))
        if img2 is not None:
            available_images.append(('Cat Meme', img2))
        if img3 is not None:
            available_images.append(('Dog', img3))
        
        if len(available_images) >= 2:
            # Use the first two available images
            name1, img1 = available_images[0]
            name2, img2 = available_images[1]
            
            # Resize images to the same size for blending
            height, width = 300, 300
            img1_resized = cv2.resize(img1, (width, height))
            img2_resized = cv2.resize(img2, (width, height))
            
            # Blend with different weights
            blend1 = cv2.addWeighted(img1_resized, 0.7, img2_resized, 0.3, 0)  # 70% img1, 30% img2
            blend2 = cv2.addWeighted(img1_resized, 0.5, img2_resized, 0.5, 0)  # 50% each
            blend3 = cv2.addWeighted(img1_resized, 0.3, img2_resized, 0.7, 0)  # 30% img1, 70% img2
            
            # Add some brightness to the blend
            blend4 = cv2.addWeighted(img1_resized, 0.5, img2_resized, 0.5, 30)  # 50% each + 30 brightness
            
            # Display results
            row1 = np.hstack((img1_resized, img2_resized))
            row2 = np.hstack((blend1, blend2))
            row3 = np.hstack((blend3, blend4))
            combined = np.vstack((row1, row2, row3))
            
            cv2.imshow('Image Blending Practice', combined)
            cv2.waitKey(0)
            cv2.destroyAllWindows()
            
            print(f"Successfully blended {name1} and {name2}")
            
        else:
            print("Not enough images found for blending. Creating sample images...")
            # Create sample images for blending
            img1 = np.zeros((200, 200, 3), dtype=np.uint8)
            img2 = np.zeros((200, 200, 3), dtype=np.uint8)
            
            # Make img1 red gradient
            for i in range(200):
                img1[:, i] = [0, 0, i]
            
            # Make img2 blue gradient
            for i in range(200):
                img2[i, :] = [i, 0, 0]
            
            # Blend them
            blended = cv2.addWeighted(img1, 0.5, img2, 0.5, 0)
            
            combined = np.hstack((img1, img2, blended))
            cv2.imshow('Sample Image Blending', combined)
            cv2.waitKey(0)
            cv2.destroyAllWindows()
            
    except Exception as e:
        print(f"Error in image blending: {e}")

def main():
    """Main function to run all practice exercises"""
    print("OpenCV Drawing, Text, and Pixel Operations Practice")
    print("=" * 50)
    
    # Run all practice functions
    drawing_shapes_practice()
    drawing_text_practice()
    add_text_to_existing_image()
    pixel_operations_practice()
    brightness_adjustment_practice()
    image_arithmetic_practice()
    image_blending_practice()
    
    print("\nAll practice exercises completed!")
    print("Press any key in the image windows to proceed to the next exercise.")

if __name__ == "__main__":
    main()
