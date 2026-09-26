if(UNIX OR WIN32)
	include_directories("${PROJECT_BINARY_DIR}/lib")
        include_directories("${PROJECT_SOURCE_DIR}/lib")
        link_directories("${PROJECT_SOURCE_DIR}/lib/NGT")

	add_executable(hamming-uint8 hamming-uint8.cpp)
	add_dependencies(hamming-uint8 ngt)
	target_link_libraries(hamming-uint8 ngt pthread)
endif()
