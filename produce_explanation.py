from pre_compute import *
import pickle


def top_popular_each_genre(precomputed_res: dict, item_id, device_id):
    if item_id in precomputed_res:
        return f"Top 10 Popular {precomputed_res[item_id]}"
    return None


def top_trending_each_genre(precomputed_res: dict, item_id, device_id):
    if item_id in precomputed_res:
        return f"Top 10 Trending {precomputed_res[item_id]}"
    return None


def top_among_history(precomputed_res: dict, item_id, device_id):
    if device_id in precomputed_res["user_history"]:
        user_history = precomputed_res["user_history"][device_id]
        for h in user_history:
            if h in precomputed_res["top_item_each_source_dict"]:
                if item_id in precomputed_res["top_item_each_source_dict"][h]:
                    if h in precomputed_res['item_name']:
                        item_name = precomputed_res['item_name'][h]
                    else:
                        item_name = "Unknown Item Name"
                    return f"Top10 In Users Watched \n {item_name}"
    return None


explanation_generator_list = [(top_popular_each_genre_precompute, top_popular_each_genre),
                              (top_trending_each_genre_precompute, top_trending_each_genre),
                              (top_popular_each_source_title_precompute, top_among_history)]


# Load the pre-computed result
precomputed_res_dict = dict()
for precomputed_func, _ in explanation_generator_list:
    pickle_name = f"{precomputed_func.__name__}.pickle"
    with open(pickle_name, 'rb') as file:
        precomputed_res_dict[precomputed_func.__name__] = pickle.load(file)


def get_explanations(item_id, device_id):
    explanation_func_list = []
    for precomputed_func, generator in explanation_generator_list:
        precomputed_res = precomputed_res_dict[precomputed_func.__name__]
        reason = generator(precomputed_res, item_id=item_id, device_id=device_id)
        if reason:
            explanation_func_list.append(reason)
    return explanation_func_list
