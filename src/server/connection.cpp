#include "server/connection.hpp"
#include "server/request_handler.hpp"
#include "server/request_parser.hpp"
#include "util/log.hpp"

#include <boost/assert.hpp>
#include <boost/bind.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filtering_stream.hpp>

#include <iterator>
#include <string>
#include <vector>

namespace osrm
{
namespace server
{

Connection::Connection(boost::asio::io_service &io_service, RequestHandler &handler)
    : strand(io_service), TCP_socket(io_service), request_handler(handler)
{
}

boost::asio::ip::tcp::socket &Connection::socket() { return TCP_socket; }

/// Start the first asynchronous operation for the connection.
void Connection::start()
{
    start_reading_time = std::chrono::steady_clock::now();
    first_reading_time = start_reading_time;
    TCP_socket.async_read_some(
        boost::asio::buffer(incoming_data_buffer),
        strand.wrap(boost::bind(&Connection::handle_read,
                                this->shared_from_this(),
                                boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred)));
}

void Connection::handle_read(const boost::system::error_code &error, std::size_t bytes_transferred)
{
    if (error)
    {
        return;
    }

    if (first_reading_time == start_reading_time)
    {
        first_reading_time = std::chrono::steady_clock::now();
    }
    // no error detected, let's parse the request
    http::compression_type compression_type(http::no_compression);
    RequestParser::RequestStatus result;
    std::tie(result, compression_type) =
        request_parser.parse(current_request,
                             incoming_data_buffer.data(),
                             incoming_data_buffer.data() + bytes_transferred);

    // the request has been parsed
    if (result == RequestParser::RequestStatus::valid)
    {
        current_request.endpoint = TCP_socket.remote_endpoint().address();
        start_processing_time = std::chrono::steady_clock::now();
        request_handler.HandleRequest(current_request, current_reply);
        start_writing_time = std::chrono::steady_clock::now();

        // compress the result w/ gzip/deflate if requested
        switch (compression_type)
        {
        case http::deflate_rfc1951:
            // use deflate for compression
            current_reply.headers.insert(current_reply.headers.begin(),
                                         {"Content-Encoding", "deflate"});
            compressed_output = compress_buffers(current_reply.content, compression_type);
            current_reply.set_size(static_cast<unsigned>(compressed_output.size()));
            output_buffer = current_reply.headers_to_buffers();
            output_buffer.push_back(boost::asio::buffer(compressed_output));
            break;
        case http::gzip_rfc1952:
            // use gzip for compression
            current_reply.headers.insert(current_reply.headers.begin(),
                                         {"Content-Encoding", "gzip"});
            compressed_output = compress_buffers(current_reply.content, compression_type);
            current_reply.set_size(static_cast<unsigned>(compressed_output.size()));
            output_buffer = current_reply.headers_to_buffers();
            output_buffer.push_back(boost::asio::buffer(compressed_output));
            break;
        case http::no_compression:
            // don't use any compression
            current_reply.set_uncompressed_size();
            output_buffer = current_reply.to_buffers();
            break;
        }
        // write result to stream
        boost::asio::async_write(TCP_socket,
                                 output_buffer,
                                 strand.wrap(boost::bind(&Connection::handle_write,
                                                         this->shared_from_this(),
                                                         boost::asio::placeholders::error)));
    }
    else if (result == RequestParser::RequestStatus::invalid)
    { // request is not parseable
        current_reply = http::reply::stock_reply(http::reply::bad_request);
        start_processing_time = std::chrono::steady_clock::now();
        start_writing_time = start_processing_time;

        boost::asio::async_write(TCP_socket,
                                 current_reply.to_buffers(),
                                 strand.wrap(boost::bind(&Connection::handle_write,
                                                         this->shared_from_this(),
                                                         boost::asio::placeholders::error)));
    }
    else
    {
        // we don't have a result yet, so continue reading
        TCP_socket.async_read_some(
            boost::asio::buffer(incoming_data_buffer),
            strand.wrap(boost::bind(&Connection::handle_read,
                                    this->shared_from_this(),
                                    boost::asio::placeholders::error,
                                    boost::asio::placeholders::bytes_transferred)));
    }
}

inline float delta_time_ms(std::chrono::time_point<std::chrono::steady_clock> const & lhs, std::chrono::time_point<std::chrono::steady_clock> const & rhs)
{
    return 0.000001 * std::chrono::duration_cast<std::chrono::nanoseconds>(lhs-rhs).count();
}

/// Handle completion of a write operation.
void Connection::handle_write(const boost::system::error_code &error)
{
    if (!error)
    {
        // Initiate graceful connection closure.
        boost::system::error_code ignore_error;
        TCP_socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignore_error);
        auto final_time = std::chrono::steady_clock::now();
        util::Log() << "Whole duration: "<< delta_time_ms(final_time, start_reading_time) <<
            "ms = time to first read : " << delta_time_ms(first_reading_time, start_reading_time) << // maybe worseless
            "ms + reading time: " << delta_time_ms(start_processing_time, first_reading_time) <<
            "ms + processing time: " << delta_time_ms(start_writing_time, start_processing_time) <<
            "ms + writing time: " << delta_time_ms(final_time, start_writing_time) <<
            "ms";

    }
}

std::vector<char> Connection::compress_buffers(const std::vector<char> &uncompressed_data,
                                               const http::compression_type compression_type)
{
    boost::iostreams::gzip_params compression_parameters;

    // there's a trade-off between speed and size. speed wins
    compression_parameters.level = boost::iostreams::zlib::best_speed;
    // check which compression flavor is used
    if (http::deflate_rfc1951 == compression_type)
    {
        compression_parameters.noheader = true;
    }

    std::vector<char> compressed_data;
    // plug data into boost's compression stream
    boost::iostreams::filtering_ostream gzip_stream;
    gzip_stream.push(boost::iostreams::gzip_compressor(compression_parameters));
    gzip_stream.push(boost::iostreams::back_inserter(compressed_data));
    gzip_stream.write(&uncompressed_data[0], uncompressed_data.size());
    boost::iostreams::close(gzip_stream);

    return compressed_data;
}
}
}
