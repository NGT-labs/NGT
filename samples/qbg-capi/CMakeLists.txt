if(UNIX OR WIN32)
	include_directories("${PROJECT_BINARY_DIR}/lib")
        include_directories("${PROJECT_SOURCE_DIR}/lib")
        link_directories("${PROJECT_SOURCE_DIR}/lib/NGT")

	add_executable(qbg-capi qbg-capi.cpp)
	add_dependencies(qbg-capi ngt)
	target_link_libraries(qbg-capi ngt pthread)
endif()
