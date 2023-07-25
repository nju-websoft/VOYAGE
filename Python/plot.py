import matplotlib
import matplotlib.pyplot as plt
import json
import numpy as np
import powerlaw
from scipy import stats
from scipy.stats import distributions
from matplotlib.ticker import FixedLocator
import math
import pandas as pd

from config import DATA_PATH, PLOT_SAVE_PATH


language_vocabularies = ['http://www.w3.org/2002/07/owl#', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                         'http://www.w3.org/2000/01/rdf-schema#', 'http://www.w3.org/2001/XMLSchema#',
                         'http://www.w3.org/2004/02/skos/core#']


def plot_ccdf_and_fitting(x_scatter, y_scatter, data_fit, xlabel, ylabel_scatter, s=None,
                          color_scatter='#1f77b4', marker='x', text_position=None, p_value_limit=0.1,
                          fontdict=None, save_path=None, figsize=None):
    if text_position is None:
        text_position = [1e3, 1e-2]
    if figsize is None:
        figsize = [6.4, 4.8]
    if fontdict is None:
        fontdict = {'family': 'Times New Roman', 'size': 22}

    if fontdict is not None:
        matplotlib.rcParams['font.family'] = fontdict['family']
        matplotlib.rcParams['font.size'] = fontdict['size']

    fig, ax1 = plt.subplots(figsize=figsize, constrained_layout=True)

    # fitting
    fit = powerlaw.Fit(data_fit, discrete=True)
    alpha = float(fit.power_law.alpha)
    D = float(fit.power_law.D)
    print('alpha: {}\nD: {}'.format(fit.power_law.alpha, fit.power_law.D))
    xmin = fit.power_law.xmin
    N = len(fit.data)
    p_value = distributions.kstwo.sf(D, N)
    print('xmin:', xmin, 'x>=xmin:', N)
    print('p_value:', p_value, ' p>0.1?', p_value > 0.1, 'in range:', fit.power_law.in_range())

    ax1.set_xscale('log')
    ax1.set_xlabel(xlabel, fontdict=fontdict)

    # scatter in ax1
    ax1.scatter(x_scatter, y_scatter, marker=marker, s=s, c=color_scatter)
    ax1.set_ylabel(ylabel_scatter, fontdict=fontdict)
    ax1.set_yscale('log')

    ax2 = ax1.twinx()

    # ccdf in ax2
    powerlaw.plot_ccdf(data_fit, linewidth=2, ax=ax2, label='ccdf')
    ax2.set_yscale('log')
    ax2.set_ylabel('$P(X \geq x)$', fontdict=fontdict)

    # power-law fitting in ax2
    if p_value > p_value_limit:
        x = np.array(data_fit)
        x = np.sort(x)
        n = len(x)
        xcdf = np.arange(n, 0, -1, dtype='float') / float(n)
        fcdf = (x / xmin) ** (1 - alpha)
        nc = xcdf[np.argmax(x >= xmin)]
        fcdf_norm = nc * fcdf
        idx = np.argwhere(x >= xmin).flatten()
        x, fcdf_norm = x[idx], fcdf_norm[idx]
        ax2.plot(x, fcdf_norm, color='m', linestyle='--', linewidth=3, label='power-law fitting')
        handles, labels = ax2.get_legend_handles_labels()
        selected_handles = [handles[1]]
        selected_labels = [labels[1]]
        ax2.legend(handles=selected_handles, labels=selected_labels, loc='upper right')
        # alpha
        ax2.text(text_position[0], text_position[1], '$\\alpha$={:.2f}'.format(alpha), color='m')

    # set the x-axis coordinate scale
    ax2.set_xscale('log')
    ax2.set_xlim(ax1.get_xlim())
    x_ticks_max = math.ceil(np.log10(max(x_scatter)))
    x_ticks = [10 ** i for i in range(x_ticks_max + 1)]
    ax2.xaxis.set_major_locator(FixedLocator(x_ticks))

    # set the y-axis coordinate scale
    y_ticks_max = math.ceil(np.log10(max(y_scatter)))
    y_ticks = [10 ** i for i in range(y_ticks_max + 1)]
    ax1.yaxis.set_major_locator(FixedLocator(y_ticks))
    cdf = powerlaw.cdf(data_fit, survival=True)[1]
    y_ticks_min = math.floor(np.log10(min(cdf)))
    if p_value > p_value_limit:
        y_ticks_min = min(y_ticks_min, math.floor(np.log10(min(fcdf_norm))))
    y_ticks = [10 ** i for i in range(y_ticks_min, 1)]
    ax2.yaxis.set_major_locator(FixedLocator(y_ticks))

    if save_path is not None:
        plt.savefig(save_path)
    plt.show()


def plot_two_scatter(data1, data2, xlabel, ylabel, legend, s=None, fontdict=None, figsize=None, save_path=None):
    if figsize is not None:
        plt.figure(figsize=figsize, constrained_layout=True)
    else:
        plt.figure(constrained_layout=True)

    if fontdict is None:
        fontdict = {'family': 'Times New Roman', 'size': 22}

    plt.axes(yscale='log', xscale='log')

    plt.minorticks_off()

    plt.xlabel(xlabel, fontdict=fontdict)
    plt.ylabel(ylabel, fontdict=fontdict)

    x, y = data1[0], data1[1]
    plt.scatter(x=x, y=y, s=s, clip_on=False, c='b', marker='*')  # blue

    x, y = data2[0], data2[1]
    plt.scatter(x=x, y=y, s=s, clip_on=False, marker='o', c='none', edgecolors='r')  # red

    plt.legend(legend, prop=fontdict)

    if fontdict is not None:
        plt.xticks(fontproperties=fontdict['family'], fontsize=fontdict['size'])
        plt.yticks(fontproperties=fontdict['family'], fontsize=fontdict['size'])

    if save_path is not None:
        plt.savefig(save_path)

    plt.show()


def load_data(filename):
    with open(DATA_PATH + filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


def plot(keyword):
    data, col, xlabel, y_label, save_path, text_position, legend = None, None, '', '', None, None, None
    vc1, vc2 = None, None
    figsize = [6.8, 4.3]

    deduplicated_datasets = {x['ID'] for x in load_data('deduplicated_datasets.json')}
    lod_datasets = {x['ID'] for x in load_data('deduplicated_datasets.json') if x['in LOD Cloud']}

    if keyword == 'fig1':  # fig1
        json_data = load_data('datasets.json')
        pld_dataset_dict = {}
        for it in json_data:
            for pld in it['PLDs']:
                ds_set = pld_dataset_dict.get(pld, set())
                ds_set.add(it['ID'])
                pld_dataset_dict[pld] = ds_set
        data = {'pld': [], 'dataset_count': []}
        for k, v in pld_dataset_dict.items():
            data['pld'].append(k)
            data['dataset_count'].append(len(set(v) & deduplicated_datasets))
        col = 'dataset_count'
        xlabel, y_label = 'Number of datasets per PLD', 'Number of PLDs'
        text_position = [5e2, 1e-1]
        save_path = PLOT_SAVE_PATH + 'pld_dataset_comprehensiveness.pdf'
    elif keyword == 'fig2':  # fig2
        json_data = load_data('vocabularies.json')
        data = {'vocabulary': [], 'term_count': []}
        for it in json_data:
            data['vocabulary'].append(it['vocabulary'])
            data['term_count'].append(len(it['classes']+it['properties']))
        col = 'term_count'
        xlabel, y_label = 'Number of terms per vocabulary', 'Number of vocabularies'
        save_path = PLOT_SAVE_PATH + 'vocab_term_comprehensiveness.pdf'
        text_position = [1e3, 1e-2]
    elif keyword == 'fig3':  # fig3
        json_data = load_data('vocabularies.json')
        data = {'vocabulary': [], 'dataset_count': []}
        for it in json_data:
            if it['vocabulary'] in language_vocabularies:
                continue
            data['vocabulary'].append(it['vocabulary'])
            data['dataset_count'].append(len(set(it['used in dataset IDs']) & deduplicated_datasets))
        col = 'dataset_count'
        xlabel, y_label = 'Number of datasets per vocabulary', 'Number of vocabularies'
        save_path = PLOT_SAVE_PATH + 'vocab_dataset_comprehensiveness.pdf'
        text_position = [1e3, 1e-3]
    elif keyword == 'fig4':  # fig4
        json_data = load_data('terms.json')
        dataset_dict = {}
        for it in json_data:
            datasets = set(it['used in dataset IDs']) & deduplicated_datasets
            for ds in datasets:
                dataset_dict[ds] = dataset_dict.get(ds, 0) + 1
        data = {'dataset_id': [], 'term_count': []}
        for k, v in dataset_dict.items():
            data['dataset_id'].append(k)
            data['term_count'].append(v)
        col = 'term_count'
        xlabel, y_label = 'Number of terms per dataset', 'Number of datasets'
        save_path = PLOT_SAVE_PATH + 'dataset_term_comprehensiveness.pdf'
        text_position = [1e3, 1e-2]
    elif keyword == 'fig5':  # fig5
        json_data = load_data('vocabularies.json')
        dataset_dict = {}
        for it in json_data:
            if it['vocabulary'] in language_vocabularies:
                continue
            datasets = set(it['used in dataset IDs']) & deduplicated_datasets
            for ds in datasets:
                dataset_dict[ds] = dataset_dict.get(ds, 0) + 1
        data = {'dataset_id': [], 'vocabulary_count': []}
        for k, v in dataset_dict.items():
            data['dataset_id'].append(k)
            data['vocabulary_count'].append(v)
        col = 'vocabulary_count'
        xlabel, y_label = 'Number of vocabularies per dataset', 'Number of datasets'
        save_path = PLOT_SAVE_PATH + 'dataset_vocab_comprehensiveness.pdf'
        text_position = [2e1, 1e-3]
    elif keyword == 'fig7':  # fig7
        json_data = load_data('edps.json')
        data = {'dataset_count': []}
        for it in json_data:
            data['dataset_count'].append(len(set(it['used in dataset IDs']) & deduplicated_datasets))
        col = 'dataset_count'
        xlabel, y_label = 'Number of datasets per EDP', 'Number of EDPs'
        text_position = [5e1, 1e-3]
        save_path = PLOT_SAVE_PATH + 'edp_dataset_term_level_comprehensiveness.pdf'
    elif keyword == 'fig8':  # fig8
        json_data = load_data('edps.json')
        data = {'dataset_count': []}
        for it in json_data:
            cnt = len(set(it['used in dataset IDs']) & lod_datasets)
            if cnt > 0:
                data['dataset_count'].append(cnt)
        col = 'dataset_count'
        xlabel, y_label = 'Number of datasets per EDP', 'Number of EDPs'
        text_position = [5e0, 3e-3]
        save_path = PLOT_SAVE_PATH + 'edp_dataset_term_level_lod_comprehensiveness.pdf'
    elif keyword == 'fig9':  # fig9
        json_data = load_data('edps.json')
        dataset_dict = {}
        for it in json_data:
            datasets = set(it['used in dataset IDs']) & deduplicated_datasets
            for ds in datasets:
                dataset_dict[ds] = dataset_dict.get(ds, 0) + 1
        data = {'dataset_id': [], 'edp_count': []}
        for k, v in dataset_dict.items():
            data['dataset_id'].append(k)
            data['edp_count'].append(v)
        col = 'edp_count'
        xlabel, y_label = 'Number of distinct EDPs per dataset', 'Number of datasets'
        text_position = [1e3, 1e-2]
        save_path = PLOT_SAVE_PATH + 'dataset_edp_comprehensiveness.pdf'
    elif keyword == 'fig10':  # fig10
        json_data = load_data('edps.json')
        dataset_dict = {}
        for it in json_data:
            datasets = set(it['used in dataset IDs']) & deduplicated_datasets
            for ds in datasets:
                dataset_dict[ds] = dataset_dict.get(ds, 0) + 1
        data = {'dataset_id': [], 'edp_count': []}
        for k, v in dataset_dict.items():
            data['dataset_id'].append(k)
            data['edp_count'].append(v)
        df = pd.DataFrame(data)
        lod_df, others_df = df[df['dataset_id'].isin(lod_datasets)], df[~df['dataset_id'].isin(lod_datasets)]
        vc1, vc2 = lod_df['edp_count'].value_counts(), others_df['edp_count'].value_counts()
        xlabel, y_label = 'Number of distinct EDPs per dataset', 'Number of datasets'
        legend = ['LOD Cloud', 'Others']
        save_path = PLOT_SAVE_PATH + 'dataset_edp_term_level.pdf'

    # plot
    print(f'Now plotting [{keyword}] ...')
    if keyword == 'fig10':
        plot_two_scatter(data1=(vc1.index, vc1.values), data2=(vc2.index, vc2.values), xlabel=xlabel, ylabel=y_label,
                         legend=legend, s=60, figsize=figsize, save_path=save_path)
    else:
        df = pd.DataFrame(data)
        vc = df[col].value_counts()
        data_fit = df[col]
        plot_ccdf_and_fitting(x_scatter=vc.index, y_scatter=vc.values, data_fit=data_fit,
                              xlabel=xlabel, ylabel_scatter=y_label, figsize=figsize,
                              text_position=text_position, save_path=save_path)


if __name__ == '__main__':
    plot('fig1')
