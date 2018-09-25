set(SOURCES
  symbol_conflict.c
)

include(${PROJECT_SOURCE_DIR}/src/build-defs.cmake)

set(TESTS_LIB_NAME ${PROJECT_NAME}-tests)
add_library(${TESTS_LIB_NAME} OBJECT ${SOURCES})
set_target_properties(${TESTS_LIB_NAME} PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Set the MODULE_NAME for the symbol conflict test (see symbol_conflict.c)
target_compile_definitions(${TESTS_LIB_NAME} PUBLIC MODULE_NAME=timescaledb)
target_include_directories(${TESTS_LIB_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(bgw)
add_subdirectory(net)
add_subdirectory(telemetry)

if (NOT WIN32)
  # The loader tests currently do not build on Windows due
  # to a reference to IsParallelWorker, which is an
  # unexported symbol
  add_subdirectory(loader)
endif (NOT WIN32)
