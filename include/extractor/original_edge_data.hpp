#ifndef ORIGINAL_EDGE_DATA_HPP
#define ORIGINAL_EDGE_DATA_HPP

#include "extractor/guidance/turn_instruction.hpp"
#include "extractor/travel_mode.hpp"
#include "util/typedefs.hpp"

#include <cstddef>
#include <limits>

namespace osrm
{
namespace extractor
{

struct OriginalEdgeData
{
    explicit OriginalEdgeData(NodeID via_node,
                              unsigned name_id,
                              guidance::TurnInstruction turn_instruction,
                              std::uint16_t entry_class,
                              TravelMode travel_mode)
        : via_node(via_node), name_id(name_id), turn_instruction(turn_instruction),
          entry_class(entry_class), travel_mode(travel_mode)
    {
    }

    OriginalEdgeData()
        : via_node(std::numeric_limits<unsigned>::max()),
          name_id(std::numeric_limits<unsigned>::max()),
          turn_instruction(guidance::TurnInstruction::INVALID()),
          entry_class(std::numeric_limits<std::uint16_t>::max()),
          travel_mode(TRAVEL_MODE_INACCESSIBLE)
    {
    }

    NodeID via_node;
    unsigned name_id;
    guidance::TurnInstruction turn_instruction;
    std::uint16_t entry_class;
    TravelMode travel_mode;
};
}
}

#endif // ORIGINAL_EDGE_DATA_HPP
