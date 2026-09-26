if(UNIX OR WIN32)
	include_directories("${PROJECT_BINARY_DIR}/lib")
        include_directories("${PROJECT_SOURCE_DIR}/lib")
        link_directories("${PROJECT_SOURCE_DIR}/lib/NGT")

	add_executable(qg-l2-float qg-l2-float.cpp)
	add_dependencies(qg-l2-float ngt)
	target_link_libraries(qg-l2-float ngt pthread)
endif()
