if [ -d "build" ]; then
    echo "Build folder exists."
else
    echo "Creating build folder..."
    mkdir build
fi

echo "Creating make file..."
cmake -S . -B build/
echo "Done!"
cd build
echo "Building..."
make 
echo "Done!"
