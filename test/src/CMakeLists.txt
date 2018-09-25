set(SOURCES
  symbol_conflict.c
)

set(TESTS_LIB_NAME ${PROJECT_NAME}-tests)
add_library(${TESTS_LIB_NAME} STATIC ${SOURCES})

# Make sure symbols are relocatable in tests lib
target_compile_options(${TESTS_LIB_NAME} PRIVATE -fPIC)
# Set the MODULE_NAME for the symbol conflict test (see symbol_conflict.c)
target_compile_definitions(${TESTS_LIB_NAME} PUBLIC MODULE_NAME=timescaledb)
target_include_directories(${TESTS_LIB_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

include(${PROJECT_SOURCE_DIR}/src/build-defs.cmake)

add_subdirectory(bgw)
add_subdirectory(net)
add_subdirectory(telemetry)

if (NOT WIN32)
  add_subdirectory(loader)
endif (NOT WIN32)
