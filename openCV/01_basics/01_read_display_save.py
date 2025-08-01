"""
Lesson 1: Reading, Displaying, and Saving Images
Learn the fundamental operations for working with images in OpenCV.
"""

import cv2
import matplotlib.pyplot as plt
import numpy as np

def lesson_1_basics():
    """Learn basic image operations"""
    
    print("📚 Lesson 1: Reading, Displaying, and Saving Images")
    print("=" * 50)
    
    # Create a sample image since we don't have one yet
    print("\n🎨 Creating a sample image...")
    sample_img = np.zeros((400, 600, 3), dtype=np.uint8)
    
    # Add some colorful patterns
    sample_img[0:133, :] = (255, 100, 100)  # Light blue section
    sample_img[133:266, :] = (100, 255, 100)  # Light green section
    sample_img[266:400, :] = (100, 100, 255)  # Light red section
    
    # Add some text
    cv2.putText(sample_img, 'OpenCV Learning', (150, 200), 
                cv2.FONT_HERSHEY_SIMPLEX, 2, (255, 255, 255), 3)
    
    # Save the sample image
    cv2.imwrite('../images/sample.jpg', sample_img)
    print("✅ Sample image created and saved as 'images/sample.jpg'")
    
    # 1. Reading an image
    print("\n📖 1. Reading an image:")
    img = cv2.imread('../images/sample.jpg')
    
    if img is None:
        print("❌ Could not read image. Check the file path.")
        return
    
    print(f"✅ Image loaded successfully!")
    print(f"   - Shape: {img.shape}")
    print(f"   - Data type: {img.dtype}")
    print(f"   - Size: {img.size} pixels")
    
    # 2. Image properties
    print("\n📏 2. Image Properties:")
    height, width, channels = img.shape
    print(f"   - Height: {height} pixels")
    print(f"   - Width: {width} pixels")
    print(f"   - Channels: {channels}")
    
    # 3. Displaying image using matplotlib (safer than cv2.imshow)
    print("\n🖼️ 3. Displaying the image:")
    img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)  # Convert BGR to RGB
    
    plt.figure(figsize=(10, 6))
    plt.subplot(1, 2, 1)
    plt.imshow(img_rgb)
    plt.title('Original Image')
    plt.axis('off')
    
    # 4. Convert to grayscale
    print("\n⚫ 4. Converting to grayscale:")
    gray_img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    print(f"   - Grayscale shape: {gray_img.shape}")
    
    plt.subplot(1, 2, 2)
    plt.imshow(gray_img, cmap='gray')
    plt.title('Grayscale Image')
    plt.axis('off')
    
    plt.tight_layout()
    plt.savefig('../images/lesson1_result.png', dpi=150, bbox_inches='tight')
    plt.show()
    
    # 5. Saving images
    print("\n💾 5. Saving images:")
    cv2.imwrite('../images/sample_grayscale.jpg', gray_img)
    print("✅ Grayscale image saved as 'images/sample_grayscale.jpg'")
    
    print("\n🎯 Key Takeaways:")
    print("   • cv2.imread() - Read images")
    print("   • cv2.imwrite() - Save images")
    print("   • cv2.cvtColor() - Convert color spaces")
    print("   • Use matplotlib for safe image display")
    print("   • OpenCV uses BGR color format by default")

# Exercise for practice
def exercise_1():
    """Practice exercise"""
    print("\n🏋️ Exercise 1: Try these tasks")
    print("1. Create your own colored image using np.zeros()")
    print("2. Add different shapes using cv2.rectangle(), cv2.circle()")
    print("3. Save it and reload it")
    print("4. Convert it to different color spaces")
    
    # Your code here...
    pass

if __name__ == "__main__":
    # Create images directory if it doesn't exist
    import os
    os.makedirs('../images', exist_ok=True)
    
    lesson_1_basics()
    exercise_1()
