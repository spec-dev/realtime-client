const webpack = require('webpack')
const path = require('path')

module.exports = {
    entry: './src/index.ts',
    output: {
        path: path.resolve(__dirname, 'dist/umd'),
        filename: 'realtime-client.js',
        library: {
            type: 'umd',
            name: 'realtime-client',
        },
    },
    module: {
        rules: [
            {
                test: /\.ts$/,
                loader: 'ts-loader',
                options: {
                    transpileOnly: true,
                },
            },
        ],
    },
    resolve: {
        extensions: ['.ts', '.js', '.json'],
    },
    plugins: [
        new webpack.DefinePlugin({
            process: 'process/browser',
        }),
    ],
}
