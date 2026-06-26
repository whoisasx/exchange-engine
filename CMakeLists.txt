cmake_minimum_required(VERSION 3.20)

project(core_engine
  VERSION 0.1.0
  LANGUAGES CXX
)

set(CORE_ENGINE_SOURCES
  src/core/EngineCore.cpp
  src/core/FarPriceMap.cpp
  src/core/FixedPoint.cpp
  src/core/IdempotencyIndex.cpp
  src/core/OrderBook.cpp
  src/core/PriceLevel.cpp
  src/core/RingPriceLadder.cpp
  src/core/SequenceGenerator.cpp
  src/core/SideBook.cpp
  src/core/Validation.cpp
)

add_library(core_engine STATIC ${CORE_ENGINE_SOURCES})
add_library(cex::core_engine ALIAS core_engine)

target_compile_features(core_engine PUBLIC cxx_std_20)

target_include_directories(core_engine
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include/core
)

add_library(engine_adapter
  src/adapter/EngineAdapter.cpp
)
add_library(cex::engine_adapter ALIAS engine_adapter)

target_compile_features(engine_adapter PUBLIC cxx_std_20)

target_include_directories(engine_adapter
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(engine_adapter
  PUBLIC
    cex::core_engine
)

add_library(engine_protocol
  src/protocol/Json.cpp
  src/protocol/Message.cpp
)
add_library(cex::engine_protocol ALIAS engine_protocol)

target_compile_features(engine_protocol PUBLIC cxx_std_20)

target_include_directories(engine_protocol
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

add_library(engine_runtime
  src/runtime/EngineEventTranslator.cpp
  src/runtime/EngineInputParser.cpp
  src/runtime/EngineOutbox.cpp
  src/runtime/EngineOutputSerializer.cpp
  src/runtime/EngineRuntime.cpp
)
add_library(cex::engine_runtime ALIAS engine_runtime)

target_compile_features(engine_runtime PUBLIC cxx_std_20)

target_include_directories(engine_runtime
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(engine_runtime
  PUBLIC
    cex::core_engine
    cex::engine_adapter
    cex::engine_protocol
)

add_library(engine_checkpoint
  src/checkpoint/EngineCheckpointManager.cpp
  src/checkpoint/file/FileCheckpointStore.cpp
)
add_library(cex::engine_checkpoint ALIAS engine_checkpoint)

target_compile_features(engine_checkpoint PUBLIC cxx_std_20)

target_include_directories(engine_checkpoint
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(engine_checkpoint
  PUBLIC
    cex::engine_runtime
)

add_library(engine_broker
  src/broker/RedpandaEngineApp.cpp
)
add_library(cex::engine_broker ALIAS engine_broker)

target_compile_features(engine_broker PUBLIC cxx_std_20)

target_include_directories(engine_broker
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(engine_broker
  PUBLIC
    cex::engine_runtime
)

add_library(engine_recovery
  src/recovery/RecoveryCoordinator.cpp
)
add_library(cex::engine_recovery ALIAS engine_recovery)

target_compile_features(engine_recovery PUBLIC cxx_std_20)

target_include_directories(engine_recovery
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(engine_recovery
  PUBLIC
    cex::engine_broker
    cex::engine_checkpoint
)

add_library(engine_app_config
  src/app/EngineAppConfig.cpp
)
add_library(cex::engine_app_config ALIAS engine_app_config)

target_compile_features(engine_app_config PUBLIC cxx_std_20)

target_include_directories(engine_app_config
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/app>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(engine_app_config
  PUBLIC
    cex::engine_runtime
)

set(ENGINE_RDKAFKA_ROOT_HINTS)
if(DEFINED ENV{HOMEBREW_PREFIX})
  list(APPEND ENGINE_RDKAFKA_ROOT_HINTS "$ENV{HOMEBREW_PREFIX}/opt/librdkafka")
endif()
list(APPEND ENGINE_RDKAFKA_ROOT_HINTS
  /opt/homebrew/opt/librdkafka
  /usr/local/opt/librdkafka
)

find_path(ENGINE_RDKAFKA_INCLUDE_DIR
  NAMES librdkafka/rdkafkacpp.h
  HINTS ${ENGINE_RDKAFKA_ROOT_HINTS}
  PATH_SUFFIXES include
)

find_library(ENGINE_RDKAFKA_CPP_LIBRARY
  NAMES rdkafka++
  HINTS ${ENGINE_RDKAFKA_ROOT_HINTS}
  PATH_SUFFIXES lib
)

find_library(ENGINE_RDKAFKA_LIBRARY
  NAMES rdkafka
  HINTS ${ENGINE_RDKAFKA_ROOT_HINTS}
  PATH_SUFFIXES lib
)

set(ENGINE_RDKAFKA_FOUND FALSE)
if(ENGINE_RDKAFKA_INCLUDE_DIR
    AND ENGINE_RDKAFKA_CPP_LIBRARY
    AND ENGINE_RDKAFKA_LIBRARY)
  set(ENGINE_RDKAFKA_FOUND TRUE)
endif()

option(ENGINE_ENABLE_RDKAFKA
  "Build the concrete librdkafka-backed broker implementation"
  ${ENGINE_RDKAFKA_FOUND}
)

if(ENGINE_ENABLE_RDKAFKA AND ENGINE_RDKAFKA_FOUND)
  add_library(RdKafka::rdkafka UNKNOWN IMPORTED)
  set_target_properties(RdKafka::rdkafka PROPERTIES
    IMPORTED_LOCATION "${ENGINE_RDKAFKA_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${ENGINE_RDKAFKA_INCLUDE_DIR}"
  )

  add_library(RdKafka::rdkafka_cpp UNKNOWN IMPORTED)
  set_target_properties(RdKafka::rdkafka_cpp PROPERTIES
    IMPORTED_LOCATION "${ENGINE_RDKAFKA_CPP_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${ENGINE_RDKAFKA_INCLUDE_DIR}"
    INTERFACE_LINK_LIBRARIES RdKafka::rdkafka
  )

  add_library(engine_broker_rdkafka
    src/broker/rdkafka/RdKafkaEngineBroker.cpp
  )
  add_library(cex::engine_broker_rdkafka ALIAS engine_broker_rdkafka)

  target_compile_features(engine_broker_rdkafka PUBLIC cxx_std_20)

  target_include_directories(engine_broker_rdkafka
    PUBLIC
      $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
      $<INSTALL_INTERFACE:include>
  )

  target_link_libraries(engine_broker_rdkafka
    PUBLIC
      cex::engine_broker
    PRIVATE
      RdKafka::rdkafka_cpp
  )

  message(STATUS "Building engine_broker_rdkafka with ${ENGINE_RDKAFKA_CPP_LIBRARY}")
elseif(NOT ENGINE_RDKAFKA_FOUND)
  message(STATUS "librdkafka++ not found; skipping engine_broker_rdkafka target/tests")
else()
  message(STATUS "ENGINE_ENABLE_RDKAFKA is OFF; skipping engine_broker_rdkafka target/tests")
endif()

if(TARGET cex::engine_broker_rdkafka)
  add_executable(engine_app
    src/app/main.cpp
  )

  target_link_libraries(engine_app
    PRIVATE
      cex::engine_app_config
      cex::engine_recovery
      cex::engine_broker_rdkafka
  )
else()
  message(STATUS "engine_app requires engine_broker_rdkafka; skipping executable")
endif()

include(CTest)

if(BUILD_TESTING)
  add_executable(adapter_tests
    tests/adapter/AdapterTests.cpp
  )

  target_link_libraries(adapter_tests
    PRIVATE
      cex::engine_adapter
  )

  add_test(NAME adapter_tests COMMAND adapter_tests)

  add_executable(protocol_fixture_tests
    tests/protocol/protocol_fixture_tests.cpp
  )

  target_link_libraries(protocol_fixture_tests
    PRIVATE
      cex::engine_protocol
  )

  target_compile_definitions(protocol_fixture_tests
    PRIVATE
      PROTOCOL_EXAMPLES_DIR="${CMAKE_CURRENT_SOURCE_DIR}/docs/examples"
  )

  add_test(
    NAME protocol_fixture_tests
    COMMAND protocol_fixture_tests
  )

  add_executable(runtime_tests
    tests/runtime/RuntimePlaceOrderTests.cpp
  )

  target_link_libraries(runtime_tests
    PRIVATE
      cex::engine_runtime
  )

  target_compile_definitions(runtime_tests
    PRIVATE
      PROTOCOL_EXAMPLES_DIR="${CMAKE_CURRENT_SOURCE_DIR}/docs/examples"
  )

  add_test(NAME runtime_tests COMMAND runtime_tests)

  add_executable(engine_output_serializer_tests
    tests/runtime/EngineOutputSerializerTests.cpp
  )

  target_link_libraries(engine_output_serializer_tests
    PRIVATE
      cex::engine_runtime
  )

  add_test(NAME engine_output_serializer_tests
           COMMAND engine_output_serializer_tests)

  add_executable(engine_smoke
    tests/smoke/EngineSmoke.cpp
  )

  target_link_libraries(engine_smoke
    PRIVATE
      cex::engine_runtime
  )

  target_compile_definitions(engine_smoke
    PRIVATE
      PROTOCOL_EXAMPLES_DIR="${CMAKE_CURRENT_SOURCE_DIR}/docs/examples"
  )

  add_test(NAME engine_smoke COMMAND engine_smoke)

  add_executable(checkpoint_tests
    tests/checkpoint/CheckpointTests.cpp
  )

  target_link_libraries(checkpoint_tests
    PRIVATE
      cex::engine_checkpoint
  )

  add_test(NAME checkpoint_tests COMMAND checkpoint_tests)

  add_executable(checkpoint_file_tests
    tests/checkpoint/file/FileCheckpointStoreTests.cpp
  )

  target_link_libraries(checkpoint_file_tests
    PRIVATE
      cex::engine_checkpoint
  )

  add_test(NAME checkpoint_file_tests COMMAND checkpoint_file_tests)

  add_executable(broker_tests
    tests/broker/RedpandaEngineAppTests.cpp
  )

  target_link_libraries(broker_tests
    PRIVATE
      cex::engine_broker
  )

  target_compile_definitions(broker_tests
    PRIVATE
      PROTOCOL_EXAMPLES_DIR="${CMAKE_CURRENT_SOURCE_DIR}/docs/examples"
  )

  add_test(NAME broker_tests COMMAND broker_tests)

  add_executable(recovery_tests
    tests/recovery/RecoveryCoordinatorTests.cpp
  )

  target_link_libraries(recovery_tests
    PRIVATE
      cex::engine_recovery
  )

  add_test(NAME recovery_tests COMMAND recovery_tests)

  if(TARGET cex::engine_broker_rdkafka)
    add_executable(rdkafka_broker_tests
      tests/broker/rdkafka/RdKafkaBackendTests.cpp
    )

    target_link_libraries(rdkafka_broker_tests
      PRIVATE
        cex::engine_broker_rdkafka
    )

    add_test(NAME rdkafka_broker_tests COMMAND rdkafka_broker_tests)
  endif()

  add_executable(engine_app_config_tests
    tests/app/EngineAppConfigTests.cpp
  )

  target_link_libraries(engine_app_config_tests
    PRIVATE
      cex::engine_app_config
  )

  add_test(NAME engine_app_config_tests COMMAND engine_app_config_tests)

endif()
