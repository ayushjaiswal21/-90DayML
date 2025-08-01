"""
OpenCV Installation Test
This script verifies that OpenCV is properly installed and working.
"""

import cv2
import numpy as np
import matplotlib.pyplot as plt

def test_opencv_installation():
    """Test OpenCV installation and basic functionality"""
    
    print("🔍 Testing OpenCV Installation...")
    print(f"OpenCV Version: {cv2.__version__}")
    
    # Test 1: Create a simple image
    print("\n✅ Test 1: Creating a simple image")
    img = np.zeros((300, 400, 3), dtype=np.uint8)
    img[:] = (100, 150, 200)  # BGR color
    print("✓ Image created successfully")
    
    # Test 2: Draw shapes
    print("\n✅ Test 2: Drawing shapes")
    cv2.rectangle(img, (50, 50), (350, 100), (0, 255, 0), 3)
    cv2.circle(img, (200, 150), 50, (255, 0, 0), -1)
    cv2.putText(img, 'OpenCV Works!', (120, 200), 
                cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)
    print("✓ Shapes drawn successfully")
    
    # Test 3: Save image
    print("\n✅ Test 3: Saving image")
    cv2.imwrite('test_image.jpg', img)
    print("✓ Image saved as 'test_image.jpg'")
    
    # Test 4: Display with matplotlib (safer than cv2.imshow)
    print("\n✅ Test 4: Displaying image")
    img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    plt.figure(figsize=(8, 6))
    plt.imshow(img_rgb)
    plt.title('OpenCV Installation Test - Success!')
    plt.axis('off')
    plt.savefig('opencv_test_result.png', dpi=150, bbox_inches='tight')
    plt.show()
    print("✓ Image displayed and saved as 'opencv_test_result.png'")
    
    print("\n🎉 All tests passed! OpenCV is working correctly.")
    print("📚 You're ready to start your OpenCV learning journey!")

if __name__ == "__main__":
    try:
        test_opencv_installation()
    except ImportError as e:
        print(f"❌ Error: {e}")
        print("📦 Please install OpenCV using: pip install opencv-python")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        print("🔧 Please check your installation and try again.")
